package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type KVServer struct {
	mu      sync.Mutex
	rwmu    sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DataBase map[string]string
	// HistoryData map[int64]string
	HistoryTran map[int64]map[int]int
	CurTran     map[int64]int
	CommitedOp  []Op
	// RaftTerm    int
}

func (kv *KVServer) Get(args *KVArgs, reply *KVReply) {
	// Your code here.
	fmt.Printf("srv%v reci %v's Get[%v] Trans:%v\n", kv.me, args.ClientID, args.Key, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value, OK}
	kv.submitCommand(&command)

	reply.ServerID = kv.me
	reply.Err = command.Status
	if reply.Err != OK {
		fmt.Printf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
	reply.Value = command.Value

	// kv.HistoryTran[args.ClientID] = args.TranscationID
}

func (kv *KVServer) Put(args *KVArgs, reply *KVReply) {
	// Your code here.
	fmt.Printf("srv%v reci %v's Put[%v]=%v Trans:%v\n", kv.me, args.ClientID, args.Key, args.Value, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value, OK}
	kv.submitCommand(&command)

	reply.ServerID = kv.me
	reply.Err = command.Status
	if reply.Err != OK {
		fmt.Printf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
	// reply.Value = command.Value

	// kv.HistoryTran[args.ClientID] = args.TranscationID
	// kv.DataBase[args.Key] = args.Value
}

func (kv *KVServer) Append(args *KVArgs, reply *KVReply) {
	// Your code here.
	fmt.Printf("srv%v reci %v's Append[%v]+%v Trans:%v\n", kv.me, args.ClientID, args.Key, args.Value, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value, OK}
	kv.submitCommand(&command)

	reply.ServerID = kv.me
	reply.Err = command.Status
	if reply.Err != OK {
		fmt.Printf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}

	// kv.HistoryTran[args.ClientID] = args.TranscationID
	// value, exists := kv.HistoryData[args.ClientID]
	// if !exists {
	// 	kv.HistoryData[args.ClientID] = kv.DataBase[args.Key]
	// 	reply.Value = kv.DataBase[args.Key]
	// 	kv.DataBase[args.Key] += args.Value
	// } else {
	// 	reply.Value = value
	// }
}

// func (kv *KVServer) Report(args *KVArgs, reply *KVReply) {
// 	// Your code here.
// 	fmt.Printf("srv%v reci %v's Report Trans:%v\n", kv.me, args.ClientID, args.TranscationID)
// 	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value, OK}
// 	kv.submitCommand(&command)

// 	reply.ServerID = kv.me
// 	reply.Err = command.Status
// 	if reply.Err != OK {
// 		fmt.Printf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
// 		return
// 	}
// 	reply.Value = command.Value

// 	// kv.mu.Lock()
// 	// defer kv.mu.Unlock()
// 	// delete(kv.HistoryData, args.ClientID)
// }

func (kv *KVServer) submitCommand(cmd *Op) {
	kv.rwmu.RLock()
	// unreliable环境，需要检查提交是否重复
	// client没收到有多种情况。1.没发出去2.发出去但还在执行3.执行完了没传回来
	if len(kv.HistoryTran[cmd.ClientID]) >= cmd.TranscationID {
		// 已执行完成的cmd，直接返回已有结果
		fmt.Printf("completed cmd%v, return\n", cmd)
		cmd.Status = ErrCompleted
		if cmd.Type == GET {
			index := kv.HistoryTran[cmd.ClientID][cmd.TranscationID]
			cmd.Value = kv.CommitedOp[index].Value
			return
		}
	}
	if kv.CurTran[cmd.ClientID] <
	cmdindex, term, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		cmd.Status = ErrWrongLeader
		return
	}

	fmt.Printf("srv:%v submit cmd%v, index:%v term%v\n", kv.me, cmd, cmdindex, term)
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		// fmt.Printf("every10ms check if%v commit\n", cmdindex)
		kv.rwmu.RLock()
		commitOp := Op{}
		if len(kv.CommitedOp) > cmdindex {
			commitOp = kv.CommitedOp[cmdindex]
		} else {
			kv.rwmu.RUnlock()
			continue
		}
		kv.rwmu.RUnlock()

		if commitOp.ClientID != cmd.ClientID {
			fmt.Printf("cmd%v index%v not commit\n", cmd, cmdindex)
			cmd.Status = ErrNotcommit
			break
		}

		cmd.Value = commitOp.Value
		break
	}
	// for !kv.killed() && kv.getAppliedLogIdx() < int32(cmdIdx) && kv.getRaftTerm() == int32(term) {
	// }

	// if cmd.Status == LogNotMatch {
	// 	DPrintf("[%s]Command %v Not Match, Need Re-Submit To KVServer", kv.getServerDetail(), cmd)
	// }

	// if != int32(term) {
	// 	cmd.Status = ErrTermchanged
	// 	DPrintf("[%s]Command %v Is Expired, SubmitTerm:%d, CurrentTerm:%d, Need Re-Submit To KVServer",
	// 		kv.getServerDetail(), cmd, term, kv.getRaftTerm())
	// }

	if kv.killed() {
		cmd.Status = ErrKilled
	}
}

func (kv *KVServer) applier(applyCh chan raft.ApplyMsg) {
	for m := range applyCh {
		// err_msg := ""
		if kv.killed() {
			return
		}
		if !m.CommandValid {
			fmt.Printf("not command,ignore\n")
		} else {
			cmdindex := m.CommandIndex
			cmd := m.Command.(Op)
			// fmt.Printf("srv:%v reci command[%v]:%v\n", kv.me, cmdindex, cmd)
			kv.applyCommand(cmdindex, &cmd)
		}
	}
}

func (kv *KVServer) applyCommand(index int, cmd *Op) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()

	kv.HistoryTran[cmd.ClientID][cmd.TranscationID] = index
	switch cmd.Type {
	case GET:
		cmd.Value = kv.DataBase[cmd.Key]
	case PUT:
		kv.DataBase[cmd.Key] = cmd.Value
	case APPEND:
		// _, exists := kv.HistoryData[cmd.ClientID]
		// if !exists {
		// kv.HistoryData[cmd.ClientID] = kv.DataBase[cmd.Key]
		kv.DataBase[cmd.Key] += cmd.Value
		// }
	case REPORT:
		// delete(kv.HistoryData, cmd.ClientID)
	}

	// if len(kv.CommitedOp) != index {
	// 	fmt.Printf("CommitedIndex wrong expect%v actal %v\n", len(kv.CommitedOp), index)
	// }
	kv.CommitedOp = append(kv.CommitedOp, *cmd)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.DataBase = make(map[string]string)
	// kv.HistoryData = make(map[int64]string)
	kv.HistoryTran = make(map[int64]map[int]int)
	kv.CurTran = make(map[int64]int)
	kv.CommitedOp = make([]Op, 1)

	go kv.applier(kv.applyCh)
	return kv
}
