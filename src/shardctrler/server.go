package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	rwmu    sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead           int32
	HistoryTran    map[int64]int
	CurCommitIndex int
	CurTerm        int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	ClientID      int64
	TranscationID int
	Type          string
	Servers       map[int][]string
	GIDs          []int
	Shard         int
	GID           int
	Num           int // desired config number
}

type OpShell struct {
	Operate *Op
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Servers: args.Servers}
	_, reply.Err = sc.submitCommand(&command)

	reply.ServerID = sc.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, GIDs: args.GIDs}
	_, reply.Err = sc.submitCommand(&command)

	reply.ServerID = sc.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Shard: args.Shard, GID: args.GID}
	_, reply.Err = sc.submitCommand(&command)

	reply.ServerID = sc.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Num: args.Num}
	reply.Config, reply.Err = sc.submitCommand(&command)

	reply.ServerID = sc.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (sc *ShardCtrler) submitCommand(cmd *Op) (Config, Err) {
	cfg := Config{}
	err := Err("")

	// 这里基本全照抄kvraft的实现
	sc.rwmu.RLock()
	if sc.HistoryTran[cmd.ClientID] >= cmd.TranscationID {
		err = ErrCompleted
		if cmd.Type == QUERY {
			cfg = Config{}
		}
		sc.rwmu.RUnlock()
		return cfg, err
	}
	sc.rwmu.RUnlock()
	cmdshell := OpShell{cmd}
	cmdindex, term, isLeader := sc.rf.Start(cmdshell)
	if !isLeader {
		err = ErrWrongLeader
		return cfg, err
	}
	DPrintf("srv:%v submit cmd%v, index:%v term%v\n", sc.me, cmd, cmdindex, term)

	for !sc.killed() {
		time.Sleep(10 * time.Millisecond)
		// DPrintf("every10ms check if%v commit\n", cmdindex)
		sc.rwmu.RLock()
		// DPrintf("kv:%v term:%v curterm:%v\n", kv.me, term, kv.CurTerm)
		if sc.CurTerm > term {
			DPrintf("commit is old term, need restart\n")
			err = ErrTermchanged
			sc.rwmu.RUnlock()
			break
		}
		// DPrintf("kv:%v curcomit%v cmdindex%v\n", kv.me, kv.CurCommitIndex, cmdindex)
		if sc.CurCommitIndex >= cmdindex {
			if sc.HistoryTran[cmd.ClientID] >= cmd.TranscationID {
				if sc.HistoryTran[cmd.ClientID] > cmd.TranscationID {
					err = ErrCompleted
				}
				if cmd.Type == QUERY {
					cfg = Config{}
				}
			} else {
				DPrintf("cmd%v index%v not commit\n", cmd, cmdindex)
				err = ErrNotcommit
			}
			sc.rwmu.RUnlock()
			break
		} else {
			sc.rwmu.RUnlock()
			continue
		}
	}
	if sc.killed() {
		err = ErrKilled
	}
	return cfg, err
}

func (sc *ShardCtrler) applier(applyCh chan raft.ApplyMsg) {
	for m := range applyCh {
		if sc.killed() {
			return
		}
		if m.SnapshotValid {
			index := m.SnapshotIndex
			term := m.SnapshotTerm
			data := m.Snapshot
			DPrintf("srv:%v reci snapshot[%v]term%v:%v\n", sc.me, index, term, data)
			// sc.applySnapshot(index, term, data)
		} else if m.CommandValid {
			cmdindex := m.CommandIndex
			cmd := m.Command.(OpShell).Operate
			DPrintf("srv:%v reci command[%v]:%v\n", sc.me, cmdindex, cmd)
			sc.applyCommand(cmdindex, cmd)
		} else {
			DPrintf("not command,ignore\n")
		}
	}
}

func (sc *ShardCtrler) applyCommand(index int, cmd *Op) {
	sc.rwmu.Lock()
	defer sc.rwmu.Unlock()
	DPrintf("apply command index%v\n", index)
	// kv.CurTerm, _ = kv.rf.GetState()
	sc.CurCommitIndex = index
	// kv.CommitedOp[index] = cmd.ClientID
	// client对不同server都进行start的情况，在apply时检查是否已提交了。
	if sc.HistoryTran[cmd.ClientID] < cmd.TranscationID {
		sc.HistoryTran[cmd.ClientID] = cmd.TranscationID
		switch cmd.Type {
		case QUERY:

		case JOIN:

		case MOVE:

		case LEAVE:

		}
	}
	// if sc.maxraftstate > 0 {
	// 	if (index+1)%30 == 0 {
	// 		DPrintf("me:%v call snapshot\n", sc.me)
	// 		w := new(bytes.Buffer)
	// 		e := labgob.NewEncoder(w)
	// 		e.Encode(sc.DataBase)
	// 		e.Encode(sc.HistoryTran)
	// 		sc.rf.Snapshot(index, w.Bytes())
	// 	}
	// }
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(OpShell{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.CurCommitIndex = 0
	sc.CurTerm = 0
	sc.HistoryTran = make(map[int64]int)
	// sc.readSnapshot(persister.ReadSnapshot())

	go sc.applier(sc.applyCh)
	go sc.termgetter()

	return sc
}

func (sc *ShardCtrler) termgetter() {
	for !sc.killed() {
		time.Sleep(1000 * time.Millisecond)
		curterm, _ := sc.rf.GetState()
		// DPrintf("me:%v check term%v leader%v\n", kv.me, curterm, isLeader)

		sc.rwmu.Lock()
		sc.CurTerm = curterm
		sc.rwmu.Unlock()
	}
}
