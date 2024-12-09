package shardctrler

import (
	"sort"
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
	configlen      int
	configs        []Config    // indexed by config num
	allocates      map[int]int // 记录每个group分配的shard数量
}

type Op struct {
	// Your data here.
	ClientID      int64
	TranscationID int
	Type          string
	Servers       map[int][]string // join
	GIDs          []int            // leave
	Shard         int              // move
	GID           int              // move
	Num           int              // query desired config number
}

type OpShell struct {
	Operate *Op
}

type BalanceKV struct {
	gid   int
	value int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Servers: args.Servers}
	_, reply.Err = sc.submitCommand(&command)

	reply.ServerID = sc.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", sc.me, command, reply.Err)
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, GIDs: args.GIDs}
	_, reply.Err = sc.submitCommand(&command)

	reply.ServerID = sc.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", sc.me, command, reply.Err)
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Shard: args.Shard, GID: args.GID}
	_, reply.Err = sc.submitCommand(&command)

	reply.ServerID = sc.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", sc.me, command, reply.Err)
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Num: args.Num}
	reply.Config, reply.Err = sc.submitCommand(&command)

	reply.ServerID = sc.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", sc.me, command, reply.Err)
		return
	}
}

func (sc *ShardCtrler) submitCommand(cmd *Op) (Config, Err) {
	cfg := Config{}
	err := Err(OK)

	// 这里基本全照抄kvraft的实现
	sc.rwmu.RLock()
	if sc.HistoryTran[cmd.ClientID] >= cmd.TranscationID {
		err = ErrCompleted
		if cmd.Type == QUERY {
			cfg = sc.execQUERY(cmd.Num)
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
					cfg = sc.execQUERY(cmd.Num)
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
			// do nothing only record
		case JOIN:
			sc.execJOIN(cmd.Servers)
		case MOVE:
			sc.execMOVE(cmd.Shard, cmd.GID)
		case LEAVE:
			sc.execLEAVE(cmd.GIDs)
		}
	}
	DPrintf("me:%v allocate:%v config:%v\n", sc.me, sc.allocates, sc.configs[sc.configlen-1])
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
	sc.configlen = 1
	sc.allocates = make(map[int]int)
	// sc.readSnapshot(persister.ReadSnapshot())

	go sc.applier(sc.applyCh)
	go sc.termgetter()

	return sc
}

func (sc *ShardCtrler) termgetter() {
	lastterm := 0
	for !sc.killed() {
		time.Sleep(1000 * time.Millisecond)
		curterm, _ := sc.rf.GetState()
		// DPrintf("me:%v check term%v leader%v\n", kv.me, curterm, isLeader)
		if curterm > lastterm {
			sc.rwmu.Lock()
			sc.CurTerm = curterm
			lastterm = curterm
			sc.rwmu.Unlock()
		}
	}
}

func (sc *ShardCtrler) execQUERY(num int) Config {
	if num == -1 || num >= sc.configlen {
		return sc.configs[sc.configlen-1]
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) execMOVE(shard int, gid int) {
	var newShards [NShards]int
	deepCopyShards(&newShards, sc.configs[sc.configlen-1].Shards)
	newGroups := deepCopyGroup(sc.configs[sc.configlen-1].Groups)

	sc.allocates[newShards[shard]]--
	newShards[shard] = gid
	sc.allocates[newShards[shard]]++

	newcfg := Config{sc.configlen, newShards, newGroups}
	sc.configs = append(sc.configs, newcfg)
	sc.configlen++
}

func (sc *ShardCtrler) execLEAVE(gids []int) {
	var newShards [NShards]int
	deepCopyShards(&newShards, sc.configs[sc.configlen-1].Shards)
	newGroups := deepCopyGroup(sc.configs[sc.configlen-1].Groups)

	DPrintf("newgroup%v\n", newGroups)
	for i := 0; i < len(gids); i++ {
		delete(newGroups, gids[i])
		delete(sc.allocates, gids[i])
		for j := 0; j < len(newShards); j++ {
			if newShards[j] == gids[i] {
				newShards[j] = 0
			}
		}
	}
	sc.balance(&newShards)
	DPrintf("afterdelete%v\n", newGroups)

	newcfg := Config{sc.configlen, newShards, newGroups}
	sc.configs = append(sc.configs, newcfg)
	sc.configlen++
}

func (sc *ShardCtrler) execJOIN(servers map[int][]string) {
	var newShards [NShards]int
	deepCopyShards(&newShards, sc.configs[sc.configlen-1].Shards)
	newGroups := deepCopyGroup(sc.configs[sc.configlen-1].Groups)

	for key, value := range servers {
		newSlice := make([]string, len(value))
		copy(newSlice, value)
		sc.allocates[key] = 0
		newGroups[key] = newSlice
	}
	DPrintf("joinbefore balance%v\n", newGroups)
	sc.balance(&newShards)
	DPrintf("joinafter balance %v\n", newGroups)
	newcfg := Config{sc.configlen, newShards, newGroups}
	sc.configs = append(sc.configs, newcfg)
	sc.configlen++
}

func deepCopyGroup(original map[int][]string) map[int][]string {
	copyMap := make(map[int][]string)
	for key, value := range original {
		newSlice := make([]string, len(value))
		copy(newSlice, value)
		copyMap[key] = newSlice
	}
	return copyMap
}

func deepCopyShards(new *[NShards]int, origin [NShards]int) {
	for i := 0; i < NShards; i++ {
		new[i] = origin[i]
	}
}

func (sc *ShardCtrler) balance(shards *[NShards]int) {
	// 1.注意balance的写法，因为map读取的随机性，在不同的server上执行会导致不同的结果
	// 一定要维持操作的一致性
	// 把allocate提取出来，按value大到小,gid小到大的顺序排序
	// 把value最多的分配给最少的
	// 2.测试中出现group大于shard数量的情况。这时group分配的shard为0
	// sc.allocates需要记录shard为0的group情况
	lengroup := len(sc.allocates)
	if lengroup == 0 {
		return
	}
	target := NShards / lengroup
	remainder := NShards % lengroup

	var sorted []BalanceKV
	for gid, v := range sc.allocates {
		sorted = append(sorted, BalanceKV{gid: gid, value: v})
	}
	// 按 value 排序，如果 value 相同，则按 gid 排序
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].value == sorted[j].value {
			return sorted[i].gid < sorted[j].gid
		}
		return sorted[i].value > sorted[j].value
	})

	for i := 0; i < len(sorted); i++ {
		gid := sorted[i].gid
		sc.allocates[gid] = i
		sorted[i].value -= target
		if remainder > 0 {
			sorted[i].value--
			remainder--
		}
	}

	flag := len(sorted) - 1
	for i := 0; i < len(shards); i++ {
		gid := shards[i]
		for flag >= 0 && sorted[flag].value == 0 {
			flag--
		}
		if flag < 0 {
			break
		}
		if index, exist := sc.allocates[gid]; !exist {
			// 未分配的shard，直接给最少group
			shards[i] = sorted[flag].gid
			sorted[flag].value++
		} else {
			if sorted[index].value > 0 {
				// 是溢出的shard，分配给最少group
				shards[i] = sorted[flag].gid
				sorted[flag].value++
				sorted[index].value--
			}
		}
	}

	// 这样写是为了保证allocates里记录了所有group情况
	for i := 0; i < len(sorted); i++ {
		gid := sorted[i].gid
		sc.allocates[gid] = 0
	}
	for i := 0; i < len(shards); i++ {
		sc.allocates[shards[i]]++
	}
}
