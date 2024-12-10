package shardkv

import (
	"bytes"
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
	isLeader       bool
	DataBase       map[int]map[string]string
	CurCommitIndex int
	CurTerm        int
	HistoryTran    map[int]map[int64]int
	shardStatus    []int
	CurConfig      shardctrler.Config
	sm             *shardctrler.Clerk
	serverID       int64
	transcationID  int
}

func (kv *ShardKV) Get(args *KVArgs, reply *KVReply) {
	// Your code here.
	// DPrintf("srv%v reci %v's Get[%v] Trans:%v\n", kv.me, args.ClientID, args.Key, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Shard, args.Key, args.Value, nil, nil}
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
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Shard, args.Key, args.Value, nil, nil}
	_, reply.Err = kv.submitCommand(&command)

	reply.ServerID = kv.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (kv *ShardKV) Append(args *KVArgs, reply *KVReply) {
	// DPrintf("srv%v reci %v's Append[%v]+%v Trans:%v\n", kv.me, args.ClientID, args.Key, args.Value, args.TranscationID)
	command := Op{args.ClientID, args.TranscationID, args.Type, args.Shard, args.Key, args.Value, nil, nil}
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
	if kv.HistoryTran[cmd.Shard][cmd.ClientID] >= cmd.TranscationID {
		// DPrintf("completed cmd%v, return\n", cmd)
		err = ErrCompleted
		if cmd.Type == GET {
			output = kv.DataBase[cmd.Shard][cmd.Key]
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
	if !kv.isLeader {
		err = ErrWrongLeader
		DPrintf("%v-%v is not group leader\n", kv.gid, kv.me)
		kv.rwmu.RUnlock()
		return output, err
	}
	if kv.shardStatus[cmd.Shard] == WAITSYNC {
		err = ErrNotReady
		DPrintf("shard:%v not ready in this group%v-%v\n", cmd.Shard, kv.gid, kv.me)
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
			if kv.HistoryTran[cmd.Shard][cmd.ClientID] >= cmd.TranscationID {
				if kv.HistoryTran[cmd.Shard][cmd.ClientID] > cmd.TranscationID {
					err = ErrCompleted
				}
				if cmd.Type == GET {
					output = kv.DataBase[cmd.Shard][cmd.Key]
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
			kv.applySnapshot(index, term, data)
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
	// DPrintf("apply command index%v\n", index)
	// kv.CurTerm, _ = kv.rf.GetState()
	kv.CurCommitIndex = index
	// kv.CommitedOp[index] = cmd.ClientID
	// client对不同server都进行start的情况，在apply时检查是否已提交了。
	if kv.HistoryTran[cmd.Shard][cmd.ClientID] < cmd.TranscationID {
		kv.HistoryTran[cmd.Shard][cmd.ClientID] = cmd.TranscationID
		switch cmd.Type {
		case PUT:
			kv.DataBase[cmd.Shard][cmd.Key] = cmd.Value
		case APPEND:
			kv.DataBase[cmd.Shard][cmd.Key] += cmd.Value
		case SYNC:
			if kv.shardStatus[cmd.Shard] == RESPONSIBLE || kv.shardStatus[cmd.Shard] == SENDSYNCWAITAGREE {
				kv.shardStatus[cmd.Shard] = SENDSYNC
			}
		case SYNCFIN:
			if kv.shardStatus[cmd.Shard] == SENDSYNC {
				kv.shardStatus[cmd.Shard] = NOTRESPONSIBLE
				delete(kv.DataBase, cmd.Shard)
				delete(kv.HistoryTran, cmd.Shard)
				kv.DataBase[cmd.Shard] = make(map[string]string)
				kv.HistoryTran[cmd.Shard] = make(map[int64]int)
			}
		case SYNCDB:
			if kv.shardStatus[cmd.Shard] == WAITSYNC || kv.shardStatus[cmd.Shard] == NOTRESPONSIBLE {
				kv.shardStatus[cmd.Shard] = RESPONSIBLE
				for k, v := range cmd.DB {
					kv.DataBase[cmd.Shard][k] = v
				}
				for k, v := range cmd.HisTran {
					kv.HistoryTran[cmd.Shard][k] = v
				}
				kv.HistoryTran[cmd.Shard][cmd.ClientID] = cmd.TranscationID
			} else {
				DPrintf("kv:%v-%v-%v not waitsync%v\n", kv.gid, kv.me, cmd.Shard, kv.shardStatus[cmd.Shard])
			}
		}
		DPrintf("kv:%v-%v-%v db%v hist%v cfg%v\n", kv.gid, kv.me, cmd.Shard, kv.DataBase[cmd.Shard], kv.HistoryTran[cmd.Shard], kv.CurConfig)
	}

	if kv.maxraftstate > 0 {
		if (index+1)%30 == 0 {
			DPrintf("me:%v call snapshot\n", kv.me)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.DataBase)
			e.Encode(kv.HistoryTran)
			e.Encode(kv.shardStatus)
			e.Encode(kv.CurConfig)
			kv.rf.Snapshot(index, w.Bytes())
		}
	}
}

func (kv *ShardKV) readSnapshot(data []byte) {
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

	if err := d.Decode(&kv.shardStatus); err != nil {
		DPrintf("me:%v Read shardstatus error:%v\n", kv.me, err)
		return
	}

	if err := d.Decode(&kv.CurConfig); err != nil {
		DPrintf("me:%v Read curcfg error:%v\n", kv.me, err)
		return
	}
}

func (kv *ShardKV) applySnapshot(index int, term int, data []byte) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	// DPrintf("apply snapshot index%v\n", index)
	kv.CurCommitIndex = index
	kv.CurTerm = term
	kv.readSnapshot(data)
	DPrintf("aftersnapshot db%v tran%v status%v cfg%v\n", kv.DataBase, kv.HistoryTran, kv.shardStatus, kv.CurConfig)
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
	kv.DataBase = make(map[int]map[string]string)
	kv.HistoryTran = make(map[int]map[int64]int)
	kv.shardStatus = make([]int, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.DataBase[i] = make(map[string]string)
		kv.HistoryTran[i] = make(map[int64]int)
		kv.shardStatus[i] = NOTRESPONSIBLE
	}
	kv.CurCommitIndex = 0
	kv.isLeader = false
	kv.CurTerm = 0
	kv.serverID = nrand()
	kv.transcationID = 0

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier(kv.applyCh)
	go kv.termgetter()
	go kv.cfggetter()

	return kv
}

func (kv *ShardKV) termgetter() {
	lastterm := 0
	lastleader := false
	for !kv.killed() {
		time.Sleep(300 * time.Millisecond)
		curterm, curleader := kv.rf.GetState()
		// DPrintf("me:%v check term%v leader%v\n", kv.me, curterm, isLeader)
		if curterm > lastterm || curleader != lastleader {
			kv.rwmu.Lock()
			kv.CurTerm = curterm
			kv.isLeader = curleader
			lastterm = curterm
			lastleader = curleader
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
			// DPrintf("me:%v-%v curconfig%v\n", kv.gid, kv.me, kv.CurConfig)
			kv.rwmu.Unlock()
		}
		kv.checkShard()
		// kv.rwmu.RLock()
		// DPrintf("me:%v-%v db%v histran%v\n", kv.gid, kv.me, kv.DataBase, kv.HistoryTran)
		// kv.rwmu.RUnlock()
	}
}

// 思考如何在server之间传递信息
// db分层后，给trans也分层记录 server之间传输时发送相应的db和trans
// 在每次config更新后,检查是否改变shards的范围
func (kv *ShardKV) checkShard() {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.shardStatus[i] == WAITSYNC || kv.shardStatus[i] == SENDSYNC {
			// 这个shard还处于传输或等待传输中，先跳过
			continue
		}
		if kv.CurConfig.Shards[i] != kv.gid && kv.shardStatus[i] == RESPONSIBLE {
			// 处于自己负责的shard,被划分到另外一个group了，需要进行同步
			// 先启动一个goroutine让自己group内的集群同步
			kv.shardStatus[i] = SENDSYNCWAITAGREE
			go kv.startsendsync(i, kv.CurConfig)
		}
		if kv.CurConfig.Shards[i] == kv.gid && kv.shardStatus[i] == NOTRESPONSIBLE {
			// 不属于自己负责shard,获取了新的shard,需要先从前一个group获取data信息
			// 如果是初始化的情况，前一任就是未分配的0，这时候可以直接开始事务处理
			// 但是如果跳过了cfg[1]的话就会错误地提前启动造成bug。
			// if kv.CurConfig.Num == 0 && cur.Num != 1 {
			// DFatal("Error:Skip Config[1]\n")
			// } else if cur.Num == 1 {
			// 初始化的情况，可以直接开始服务
			if kv.CurConfig.Num == 2 || kv.CurConfig.Num == 1 {
				kv.shardStatus[i] = RESPONSIBLE
			} else {
				kv.shardStatus[i] = WAITSYNC
			}
		}
		// 剩余的情形不需要考虑
	}
}

func (kv *ShardKV) startsendsync(shard int, cur shardctrler.Config) {
	// 对于某个需要同步的shard，首先提交到raft层，让group内部达成一致
	// 提交的command只需要类型与shard编号即可
	// 同步时，已经在本机提交的trans保留执行，执行完成后再把db和记录发送出去
	kv.rwmu.Lock()
	kv.transcationID++
	command := Op{ClientID: kv.serverID, TranscationID: kv.transcationID, Type: SYNC, Shard: shard}
	kv.rwmu.Unlock()
	flag := true
	for flag {
		// 采用循环的模式执行，因为担心raft层恰好无leader导致command提交失败
		// kv层的server不知道raft层是否存在leader,所以必须要多次尝试
		DPrintf("Try Sync shard%v In Group%v-%v\n", shard, kv.gid, kv.me)
		NeedReTry, Err := kv.submitSync(&command) // 还是和client区分开用另一个函数写方便一些
		if Err == OK || NeedReTry == NO {
			flag = false
		}
		// 由于有循环检测，所以只让leader进行submit了
		// 如果出现新的leader,在循环检测的时候它就会进行submitsync
		if Err == ErrSyncDBCompleted {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 认为同步完成，开始向目标shard发送db和cl
	kv.rwmu.RLock()
	db := make(map[string]string)
	hist := make(map[int64]int)
	for k, v := range kv.HistoryTran[shard] {
		hist[k] = v
	}
	for k, v := range kv.DataBase[shard] {
		db[k] = v
	}
	// DPrintf("kvhist%v send%v kvdb%v db%v\n", kv.HistoryTran[shard], hist, kv.DataBase[shard], db)
	args := SyncDBArgs{kv.serverID, shard, db, hist}
	reply := SyncDBReply{}
	kv.rwmu.RUnlock()

	// 在这一部分 3对3都发送syncdb,目标是让对方至少一半成功同步db
	// 在对方返回成功信息后,需要把成功信息也至少发送给一半原server
	// 有可能出现返回的leader被屏蔽后丢失command，导致原group一直处于send状态
	flag = true
	for flag {
		gid := cur.Shards[shard]
		if server, ok := cur.Groups[gid]; ok {
			for si := 0; si < len(server); si++ {
				srv := kv.make_end(server[si])
				ok := srv.Call("ShardKV.SyncDB", &args, &reply)
				if !ok || reply.Err == ErrKilled || reply.Err == ErrWrongLeader {
					continue
				}
				if reply.Err == OK {
					DPrintf("%v-%v send%v to %v-%v succeed\n", kv.gid, kv.me, shard, gid, si)
					flag = false
					break
				}
				if reply.Err == ErrCompleted {
					// fmt.Printf("cl%v %v to src%v already\n", ck.clientID, op, curserver)
					flag = false
					break
				}
				if reply.Err == ErrWrongGroup {
					break
				}
				if reply.Err == ErrNoKey || reply.Err == ErrTermchanged {
					// fmt.Printf("cl%v %v to srv%v failed:%v\n", ck.clientID, op, curserver, reply.Err)
					continue
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 收到成功的消息，对自己的server进行同步完成的消息
	kv.rwmu.Lock()
	kv.transcationID++
	command2 := Op{ClientID: kv.serverID, TranscationID: kv.transcationID, Type: SYNCFIN, Shard: shard}
	kv.rwmu.Unlock()
	flag = true
	for flag {
		// 采用循环的模式执行，因为担心raft层恰好无leader导致command提交失败
		// kv层的server不知道raft层是否存在leader,所以必须要多次尝试
		DPrintf("Try Syncfin shard%v In Group%v-%v\n", shard, kv.gid, kv.me)
		NeedReTry, Err := kv.submitSync(&command2) // 还是和client区分开用另一个函数写方便一些
		if Err == OK || NeedReTry == NO {
			flag = false
		}
		if Err == ErrSyncDBCompleted {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *ShardKV) submitSync(cmd *Op) (string, Err) {
	// 大体模仿submitcmd的函数逻辑,简单修改返回的内容
	needretry := YES
	err := Err(OK)
	kv.rwmu.RLock()
	if kv.shardStatus[cmd.Shard] != SENDSYNCWAITAGREE && cmd.Type == SYNC {
		err = ErrCompleted
		needretry = NO
		kv.rwmu.RUnlock()
		return needretry, err
	} else if kv.shardStatus[cmd.Shard] != SENDSYNC && cmd.Type == SYNCFIN {
		err = ErrCompleted
		needretry = NO
		kv.rwmu.RUnlock()
		return needretry, err
	}
	if kv.HistoryTran[cmd.Shard][cmd.ClientID] >= cmd.TranscationID {
		// DPrintf("completed cmd%v, return\n", cmd)
		err = ErrCompleted
		needretry = NO
		kv.rwmu.RUnlock()
		return needretry, err
	}
	if !kv.isLeader {
		err = ErrWrongLeader
		DPrintf("%v-%v is not group leader\n", kv.gid, kv.me)
		kv.rwmu.RUnlock()
		return needretry, err
	}
	kv.rwmu.RUnlock()
	cmdshell := OpShell{cmd}
	cmdindex, term, isLeader := kv.rf.Start(cmdshell)
	if !isLeader {
		err = ErrWrongLeader
		return needretry, err
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
		// DPrintf("kv:%v curcomit%v cmdindex%v\n", kv.me, kv.CurCommitIndex, cmdindex)
		if kv.CurCommitIndex >= cmdindex {
			if kv.HistoryTran[cmd.Shard][cmd.ClientID] >= cmd.TranscationID {
				needretry = NO
				if kv.shardStatus[cmd.Shard] != SENDSYNC && cmd.Type == SYNC {
					err = ErrSyncDBCompleted
				} else if kv.shardStatus[cmd.Shard] != NOTRESPONSIBLE && cmd.Type == SYNCFIN {
					err = ErrSyncDBCompleted
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
		needretry = NO
	}
	return needretry, err
}

func (kv *ShardKV) SyncDB(args *SyncDBArgs, reply *SyncDBReply) {
	reply.ServerID = kv.serverID
	shard := args.Shard
	kv.rwmu.Lock()
	if (kv.shardStatus[shard] != WAITSYNC) && (kv.shardStatus[shard] != NOTRESPONSIBLE) {
		DPrintf("%v-%v sync db%v completed\n", kv.gid, kv.me, shard)
		reply.Err = ErrCompleted
		kv.rwmu.Unlock()
		return
	}
	if !kv.ifNew(shard, args.HisTran) {
		DPrintf("%v-%v reci db is old,ignore\n", kv.gid, kv.me)
		reply.Err = ErrCompleted
		kv.rwmu.Unlock()
		return
	}
	kv.transcationID++
	command := Op{ClientID: kv.serverID, TranscationID: kv.transcationID, Type: SYNCDB, Shard: args.Shard, DB: args.DB, HisTran: args.HisTran}
	kv.rwmu.Unlock()
	reply.Err = kv.submitSyncDB(&command)

	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (kv *ShardKV) ifNew(shard int, db map[int64]int) bool {
	for k, v := range db {
		if v > kv.HistoryTran[shard][k] {
			return true
		}
	}
	// 这里要不要考虑kv有his没有的键值呢？没有必要，如果有也必然是false
	return false
}

func (kv *ShardKV) submitSyncDB(cmd *Op) Err {
	err := Err(OK)
	kv.rwmu.RLock()
	if kv.HistoryTran[cmd.Shard][cmd.ClientID] >= cmd.TranscationID {
		// DPrintf("completed cmd%v, return\n", cmd)
		err = ErrCompleted
		kv.rwmu.RUnlock()
		return err
	}
	if !kv.isLeader {
		err = ErrWrongLeader
		DPrintf("%v-%v is not group leader\n", kv.gid, kv.me)
		kv.rwmu.RUnlock()
		return err
	}
	kv.rwmu.RUnlock()
	cmdshell := OpShell{cmd}
	cmdindex, term, isLeader := kv.rf.Start(cmdshell)
	if !isLeader {
		err = ErrWrongLeader
		return err
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
		// DPrintf("kv:%v curcomit%v cmdindex%v\n", kv.me, kv.CurCommitIndex, cmdindex)
		if kv.CurCommitIndex >= cmdindex {
			if kv.HistoryTran[cmd.Shard][cmd.ClientID] >= cmd.TranscationID {
				if cmd.Value == ErrCompleted {
					err = ErrCompleted
				}
				if cmd.Value == ErrSyncDBCompleted {
					err = ErrSyncDBCompleted
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
	return err
}
