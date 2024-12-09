package shardkv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type ShardKV struct {
	rwmu         sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead           int32
	DataBase       map[string]string
	CurCommitIndex int
	CurTerm        int
	HistoryTran    map[int64]int
	CurConfig      shardctrler.Config
	sm             *shardctrler.Clerk
}

func (kv *ShardKV) Get(args *KVArgs, reply *KVReply) {
	// Your code here.
	// DPrintf("srv%v reci %v's Get[%v] Trans:%v\n", kv.me, args.ClientID, args.Key, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Shard, args.Value}
	reply.Value, reply.Err = kv.submitCommand(&command)

	reply.ServerID = kv.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (kv *ShardKV) Put(args *KVArgs, reply *KVReply) {
	// Your code here.
	// DPrintf("srv%v reci %v's Put[%v]=%v Trans:%v\n", kv.me, args.ClientID, args.Key, args.Value, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Shard, args.Value}
	_, reply.Err = kv.submitCommand(&command)

	reply.ServerID = kv.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (kv *ShardKV) Append(args *KVArgs, reply *KVReply) {
	// DPrintf("srv%v reci %v's Append[%v]+%v Trans:%v\n", kv.me, args.ClientID, args.Key, args.Value, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Key, args.Shard, args.Value}
	_, reply.Err = kv.submitCommand(&command)

	reply.ServerID = kv.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (kv *ShardKV) submitCommand(cmd *Op) (string, Err) {
	output := ""
	err := Err(OK)

	kv.rwmu.RLock()
	if kv.HistoryTran[cmd.ClientID] >= cmd.TranscationID {
		// DPrintf("completed cmd%v, return\n", cmd)
		err = ErrCompleted
		if cmd.Type == GET {
			output = kv.DataBase[cmd.Key]
		}
		kv.rwmu.RUnlock()
		return output, err
	}
	if kv.CurConfig.Shards[cmd.Shard] != kv.gid {
		err = ErrWrongGroup
		DPrintf("shard:%v not this group%v-%v,cfg:%v\n", cmd.Shard, kv.gid, kv.me, kv.CurConfig)
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
	DPrintf("srv:%v-%v submit cmd%v, index:%v term%v\n", kv.gid, kv.me, cmd, cmdindex, term)

	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		kv.rwmu.RLock()
		// DPrintf("kv:%v term:%v curterm:%v\n", kv.me, term, kv.CurTerm)
		if kv.CurTerm > term {
			DPrintf("commit is old term, need restart\n")
			err = ErrTermchanged
			kv.rwmu.RUnlock()
			break
		}
		if kv.CurConfig.Shards[cmd.Shard] != kv.gid {
			err = ErrWrongGroup
			DPrintf("shard:%v not this group%v-%v,cfg:%v\n", cmd.Shard, kv.gid, kv.me, kv.CurConfig)
			kv.rwmu.RUnlock()
			return output, err
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
			kv.rwmu.RUnlock()
			break
		} else {
			kv.rwmu.RUnlock()
			continue
		}
	}
	if kv.killed() {
		err = ErrKilled
	}
	return output, err
}

func (kv *ShardKV) applier(applyCh chan raft.ApplyMsg) {
	for m := range applyCh {
		// err_msg := ""
		if kv.killed() {
			return
		}
		if m.SnapshotValid {
			index := m.SnapshotIndex
			term := m.SnapshotTerm
			data := m.Snapshot
			DPrintf("srv:%v-%v reci snapshot[%v]term%v:%v\n", kv.gid, kv.me, index, term, data)
			// kv.applySnapshot(index, term, data)
		} else if m.CommandValid {
			cmdindex := m.CommandIndex
			cmd := m.Command.(OpShell).Operate
			DPrintf("srv:%v-%v reci command[%v]:%v\n", kv.gid, kv.me, cmdindex, cmd)
			kv.applyCommand(cmdindex, cmd)
		} else {
			DPrintf("not command,ignore\n")
		}
	}
}

func (kv *ShardKV) applyCommand(index int, cmd *Op) {
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
	// if kv.maxraftstate > 0 {
	// 	if (index+1)%30 == 0 {
	// 		DPrintf("me:%v call snapshot\n", kv.me)
	// 		w := new(bytes.Buffer)
	// 		e := labgob.NewEncoder(w)
	// 		e.Encode(kv.DataBase)
	// 		e.Encode(kv.HistoryTran)
	// 		kv.rf.Snapshot(index, w.Bytes())
	// 	}
	// }
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(OpShell{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.dead = 0
	kv.DataBase = make(map[string]string)
	kv.HistoryTran = make(map[int64]int)
	kv.CurCommitIndex = 0
	kv.CurTerm = 0

	// kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier(kv.applyCh)
	go kv.termgetter()
	go kv.cfggetter()

	return kv
}

func (kv *ShardKV) termgetter() {
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

// 每个server定期向ctrler获取当前的config
func (kv *ShardKV) cfggetter() {
	lastnum := 0
	curconfig := shardctrler.Config{}
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		curconfig = kv.sm.Query(-1)
		if lastnum != curconfig.Num {
			kv.rwmu.Lock()
			kv.CurConfig = curconfig
			lastnum = curconfig.Num
			DPrintf("me:%v-%v curconfig%v\n", kv.gid, kv.me, kv.CurConfig)
			kv.rwmu.Unlock()
		}
	}
}
