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
	// mu      sync.Mutex
	rwmu    sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DataBase map[string]string
	// HistoryData map[int64]string
	HistoryTran    map[int64]map[int]int
	CurCommitIndex int
	CurTerm        int
	CommitedOp     map[int]int64
	CommitedforGet map[int]string
	// RaftTerm    int
}

func (kv *KVServer) Get(args *KVArgs, reply *KVReply) {
	// Your code here.
	fmt.Printf("srv%v reci %v's Get[%v] Trans:%v\n", kv.me, args.ClientID, args.Key, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value, OK}
	kv.submitCommand(&command)

	reply.ServerID = kv.me
	reply.Err = command.Status
	if reply.Err != OK && reply.Err != ErrCompleted {
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
	if reply.Err != OK && reply.Err != ErrCompleted {
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
	if reply.Err != OK && reply.Err != ErrCompleted {
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
	// 4. client向不同的server都传递了start，如何保证最后只有一个被执行？
	// 5. 考虑死锁的问题，new term需要新commit才能commit之前的提交，如果发现重复就拒绝
	// 将无法产生新的提交导致死循环，所以即使是重复的trans，只要没有commit就得提交
	// 6.出现这种情况,start到某个index,之后leader变化,丢失log,但是新commit的没有到原长度
	// 导致原command还在不断检查的情况。
	// 修改方式是接受log时，检查log的term，如果超过了现有的term，
	// 说明过时不会commit了，让client重发
	if len(kv.HistoryTran[cmd.ClientID]) >= cmd.TranscationID {
		// 已执行完成的cmd，直接返回已有结果
		fmt.Printf("completed cmd%v, return\n", cmd)
		cmd.Status = ErrCompleted
		if cmd.Type == GET {
			index := kv.HistoryTran[cmd.ClientID][cmd.TranscationID]
			// cmd.Value = kv.CommitedOp[index].Value
			cmd.Value = kv.CommitedforGet[index]
		}
		kv.rwmu.RUnlock()
		return
	}
	kv.rwmu.RUnlock()
	cmdindex, term, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		cmd.Status = ErrWrongLeader
		return
	}
	fmt.Printf("srv:%v submit cmd%v, index:%v term%v\n", kv.me, cmd, cmdindex, term)
	var commitclient int64 = 0
	var exist bool
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		// fmt.Printf("every10ms check if%v commit\n", cmdindex)
		kv.rwmu.RLock()
		// fmt.Printf("kv:%v term:%v curterm:%v\n", kv.me, term, kv.CurTerm)
		if kv.CurTerm > term {
			fmt.Printf("commit is old term, need restart\n")
			cmd.Status = ErrTermchanged
			kv.rwmu.RUnlock()
			break
		}
		fmt.Printf("kv:%v curcomit%v cmdindex%v\n", kv.me, kv.CurCommitIndex, cmdindex)
		if kv.CurCommitIndex >= cmdindex {
			commitclient, exist = kv.CommitedOp[cmdindex]
			if !exist {
				// 提交到了cmdindex，却没有存index的clientid，说明这个cmd已经提交过了
				// 从transcid里找之前commit的
				cmd.Status = ErrCompleted
				if cmd.Type == GET {
					cmdindex = kv.HistoryTran[cmd.ClientID][cmd.TranscationID]
					cmd.Value = kv.CommitedforGet[cmdindex]
				}
				kv.rwmu.RUnlock()
				break
			}
			if cmd.Type == GET {
				cmd.Value = kv.CommitedforGet[cmdindex]
			}
		} else {
			kv.rwmu.RUnlock()
			continue
		}
		kv.rwmu.RUnlock()
		if commitclient != cmd.ClientID {
			fmt.Printf("cmd%v index%v not commit\n", cmd, cmdindex)
			cmd.Status = ErrNotcommit
			break
		}

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
	fmt.Printf("apply command index%v\n", index)
	if _, exists := kv.HistoryTran[cmd.ClientID]; !exists {
		// 如果没有初始化嵌套的 map，则初始化
		kv.HistoryTran[cmd.ClientID] = make(map[int]int)
	}
	kv.CurCommitIndex = index
	// client对不同server都进行start的情况，在apply时检查是否已提交了。
	if _, exist := kv.HistoryTran[cmd.ClientID][cmd.TranscationID]; exist {
		return
	}

	kv.HistoryTran[cmd.ClientID][cmd.TranscationID] = index
	switch cmd.Type {
	case GET:
		kv.CommitedforGet[index] = kv.DataBase[cmd.Key]
	case PUT:
		kv.DataBase[cmd.Key] = cmd.Value
	case APPEND:
		// _, exists := kv.HistoryData[cmd.ClientID]
		// if !exists {
		// kv.HistoryData[cmd.ClientID] = kv.DataBase[cmd.Key]
		kv.DataBase[cmd.Key] += cmd.Value

		// }
		// case REPORT:
		// delete(kv.HistoryData, cmd.ClientID)
	}
	kv.CommitedOp[index] = cmd.ClientID
	// if len(kv.CommitedOp) != index {
	// 	fmt.Printf("CommitedIndex wrong expect%v actal %v\n", len(kv.CommitedOp), index)
	// }
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
	kv.CurCommitIndex = 0
	kv.CommitedOp = make(map[int]int64)
	kv.CommitedforGet = make(map[int]string)
	kv.CurTerm = 0
	go kv.applier(kv.applyCh)
	go kv.termgetter()
	return kv
}

func (kv *KVServer) termgetter() {
	for !kv.killed() {
		time.Sleep(1000 * time.Millisecond)
		curterm, _ := kv.rf.GetState()
		// fmt.Printf("me:%v check term%v leader%v\n", kv.me, curterm, isLeader)

		kv.rwmu.Lock()
		kv.CurTerm = curterm
		kv.rwmu.Unlock()
	}
}
