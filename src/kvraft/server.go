package kvraft

import (
	"bytes"
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
	// 为了压缩snapshot大小,只存储最高的trans值
	// HistoryTran    map[int64]map[int]int
	HistoryTran    map[int64]int
	CurCommitIndex int
	CurTerm        int
	// CommitedOp     map[int]int64
	// CommitedforGet map[int]string
	// RaftTerm    int
}

func (kv *KVServer) Get(args *KVArgs, reply *KVReply) {
	// Your code here.
	// DPrintf("srv%v reci %v's Get[%v] Trans:%v\n", kv.me, args.ClientID, args.Key, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value}
	reply.Value, reply.Err = kv.submitCommand(&command)

	reply.ServerID = kv.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
	// kv.HistoryTran[args.ClientID] = args.TranscationID
}

func (kv *KVServer) Put(args *KVArgs, reply *KVReply) {
	// Your code here.
	// DPrintf("srv%v reci %v's Put[%v]=%v Trans:%v\n", kv.me, args.ClientID, args.Key, args.Value, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value}
	_, reply.Err = kv.submitCommand(&command)

	reply.ServerID = kv.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
	// reply.Value = command.Value

	// kv.HistoryTran[args.ClientID] = args.TranscationID
	// kv.DataBase[args.Key] = args.Value
}

func (kv *KVServer) Append(args *KVArgs, reply *KVReply) {
	// Your code here.
	// DPrintf("srv%v reci %v's Append[%v]+%v Trans:%v\n", kv.me, args.ClientID, args.Key, args.Value, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value}
	_, reply.Err = kv.submitCommand(&command)

	reply.ServerID = kv.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
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
// 	DPrintf("srv%v reci %v's Report Trans:%v\n", kv.me, args.ClientID, args.TranscationID)
// 	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Value, OK}
// 	kv.submitCommand(&command)

// 	reply.ServerID = kv.me
// 	reply.Err = command.Status
// 	if reply.Err != OK {
// 		DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
// 		return
// 	}
// 	reply.Value = command.Value

// 	// kv.mu.Lock()
// 	// defer kv.mu.Unlock()
// 	// delete(kv.HistoryData, args.ClientID)
// }

func (kv *KVServer) submitCommand(cmd *Op) (string, Err) {
	output := ""
	err := Err(OK)

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
	// 7.偶发新的getbug,get到的值为空，原因是错误判定已完成。重新检查submit的逻辑
	// 原因出在apply上，index上commit的是已commit的，所以直接返回，没有储存commitcl却更新了curindex
	// submit循环检查发现curindex大于当前值,然后查找不到commitop便以为是已提交的commit
	// 简单修改,需要存所有的cmtclid。
	// 8. 思考线性化的意义。在实现raft后应该是天然线性化的，因此没有必要为get存储过去的数据
	// 9. 新的偶发bug,只存cmtclid不够,考虑这种情况:cl向srv1提交在index900处并commit成功,
	// 之后向一个被隔离的srv提交另一个指令,index刚好也是900,此时clid一致但transid不同。
	// srv重连后向leader同步,由于是相同的clid,让srv以为这条指令已执行,结果是数据丢失
	// 修改方法:都记录clid和trid
	if kv.HistoryTran[cmd.ClientID] >= cmd.TranscationID {
		// 已执行完成的cmd，直接返回已有结果
		// DPrintf("completed cmd%v, return\n", cmd)
		err = ErrCompleted
		if cmd.Type == GET {
			// index := kv.HistoryTran[cmd.ClientID][cmd.TranscationID]
			// cmd.Value = kv.CommitedOp[index].Value
			// cmd.Value = kv.CommitedforGet[index]
			output = kv.DataBase[cmd.Key]
		}
		kv.rwmu.RUnlock()
		return output, err
	}
	kv.rwmu.RUnlock()
	cmdshell := OpShell{cmd}
	cmdindex, term, isLeader := kv.rf.Start(cmdshell)
	if !isLeader {
		err = ErrWrongLeader
		return output, err
	}
	DPrintf("srv:%v submit cmd%v, index:%v term%v\n", kv.me, cmd, cmdindex, term)

	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		// DPrintf("every10ms check if%v commit\n", cmdindex)
		kv.rwmu.RLock()
		// DPrintf("kv:%v term:%v curterm:%v\n", kv.me, term, kv.CurTerm)
		if kv.CurTerm > term {
			DPrintf("commit is old term, need restart\n")
			err = ErrTermchanged
			kv.rwmu.RUnlock()
			break
		}
		// DPrintf("kv:%v curcomit%v cmdindex%v\n", kv.me, kv.CurCommitIndex, cmdindex)
		if kv.CurCommitIndex >= cmdindex {
			if kv.HistoryTran[cmd.ClientID] >= cmd.TranscationID {
				if kv.HistoryTran[cmd.ClientID] > cmd.TranscationID {
					err = ErrCompleted
				}
				if cmd.Type == GET {
					output = kv.DataBase[cmd.Key]
				}
			} else {
				DPrintf("cmd%v index%v not commit\n", cmd, cmdindex)
				err = ErrNotcommit
			}
			// if cmd.ClientID != kv.CommitedOp[cmdindex] {
			// 	DPrintf("cmd%v index%v not commit\n", cmd, cmdindex)
			// 	cmd.Status = ErrNotcommit
			// } else {
			// 	if cmdindex != kv.HistoryTran[cmd.ClientID][cmd.TranscationID] {
			// 		cmd.Status = ErrCompleted
			// 		cmdindex = kv.HistoryTran[cmd.ClientID][cmd.TranscationID]
			// 	}
			// 	if cmd.Type == GET {
			// 		cmd.Value = kv.CommitedforGet[cmdindex]
			// 	}
			// }
			kv.rwmu.RUnlock()
			break
		} else {
			kv.rwmu.RUnlock()
			continue
		}
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
		err = ErrKilled
	}
	return output, err
}

func (kv *KVServer) applier(applyCh chan raft.ApplyMsg) {
	for m := range applyCh {
		// err_msg := ""
		if kv.killed() {
			return
		}
		if m.SnapshotValid {
			index := m.SnapshotIndex
			term := m.SnapshotTerm
			data := m.Snapshot
			kv.applySnapshot(index, term, data)
		} else if m.CommandValid {
			cmdindex := m.CommandIndex
			cmd := m.Command.(OpShell).Operate

			// DPrintf("srv:%v reci command[%v]:%v\n", kv.me, cmdindex, cmd)
			kv.applyCommand(cmdindex, cmd)
		} else {
			DPrintf("not command,ignore\n")

		}
	}
}

func (kv *KVServer) applyCommand(index int, cmd *Op) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	DPrintf("apply command index%v\n", index)
	// kv.CurTerm, _ = kv.rf.GetState()
	kv.CurCommitIndex = index
	// kv.CommitedOp[index] = cmd.ClientID
	// client对不同server都进行start的情况，在apply时检查是否已提交了。
	if kv.HistoryTran[cmd.ClientID] < cmd.TranscationID {
		kv.HistoryTran[cmd.ClientID] = cmd.TranscationID
		switch cmd.Type {
		case PUT:
			kv.DataBase[cmd.Key] = cmd.Value
		case APPEND:
			kv.DataBase[cmd.Key] += cmd.Value
		}
	}
	// kv.HistoryTran[cmd.ClientID][cmd.TranscationID] = index
	if kv.maxraftstate > 0 {
		if (index+1)%30 == 0 {
			DPrintf("me:%v call snapshot\n", kv.me)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.DataBase)
			e.Encode(kv.HistoryTran)
			kv.rf.Snapshot(index, w.Bytes())
		}
	}
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
	// labgob.Register(Op{})
	labgob.Register(OpShell{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.DataBase = make(map[string]string)
	// kv.HistoryData = make(map[int64]string)
	kv.HistoryTran = make(map[int64]int)
	kv.CurCommitIndex = 0
	// kv.CommitedOp = make(map[int]int64)
	// kv.CommitedforGet = make(map[int]string)
	kv.CurTerm = 0

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier(kv.applyCh)
	go kv.termgetter()
	return kv
}

func (kv *KVServer) termgetter() {
	lastterm := 0
	for !kv.killed() {
		time.Sleep(1000 * time.Millisecond)
		curterm, _ := kv.rf.GetState()
		// DPrintf("me:%v check term%v leader%v\n", kv.me, curterm, isLeader)
		if curterm > lastterm {
			kv.rwmu.Lock()
			kv.CurTerm = curterm
			lastterm = curterm
			kv.rwmu.Unlock()
		}
	}
}

func (kv *KVServer) readSnapshot(data []byte) {
	// DPrintf("me:%v read snapshot\n", rf.me)

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&kv.DataBase); err != nil {
		DPrintf("me:%v Read database error:%v\n", kv.me, err)
		return
	}

	if err := d.Decode(&kv.HistoryTran); err != nil {
		DPrintf("me:%v Read historytran error:%v\n", kv.me, err)
		return
	}
}

func (kv *KVServer) applySnapshot(index int, term int, data []byte) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	DPrintf("apply snapshot index%v\n", index)
	kv.CurCommitIndex = index
	kv.CurTerm = term
	kv.readSnapshot(data)
}
