package shardkv

import (
	"bytes"
	"fmt"
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
	shardSendto    []int
	allknowgroups  map[int][]string // 记录所有曾经加入过集群的group
}

func (kv *ShardKV) Get(args *KVArgs, reply *KVReply) {
	// Your code here.
	// DPrintf("srv%v reci %v's Get[%v] Trans:%v\n", kv.me, args.ClientID, args.Key, args.TranscationID)
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Shard: args.Shard, Key: args.Key}
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
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Shard: args.Shard, Key: args.Key, Value: args.Value}
	_, reply.Err = kv.submitCommand(&command)

	reply.ServerID = kv.me
	if reply.Err != OK && reply.Err != ErrCompleted {
		// DPrintf("srv%v submit command%v fail:%v\n", kv.me, command, reply.Err)
		return
	}
}

func (kv *ShardKV) Append(args *KVArgs, reply *KVReply) {
	// DPrintf("srv%v reci %v's Append[%v]+%v Trans:%v\n", kv.me, args.ClientID, args.Key, args.Value, args.TranscationID)
	command := Op{ClientID: args.ClientID, TranscationID: args.TranscationID, Type: args.Type, Shard: args.Shard, Key: args.Key, Value: args.Value}
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
	if kv.shardStatus[cmd.Shard] != RESPONSIBLE {
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
		if kv.shardStatus[cmd.Shard] != RESPONSIBLE {
			err = ErrWrongGroup
			DPrintf("shard:%v not this group%v-%v,shardstatus%v,cfg:%v\n", cmd.Shard, kv.gid, kv.me, kv.shardStatus[cmd.Shard], kv.CurConfig)
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
		tmp := kv.HistoryTran[cmd.Shard][cmd.ClientID]
		kv.HistoryTran[cmd.Shard][cmd.ClientID] = cmd.TranscationID
		switch cmd.Type {
		case GET:
			if kv.shardStatus[cmd.Shard] != RESPONSIBLE {
				// 对于不执行的put get append,把trans倒退回上次记录,submit检查时会发现notcommit,再次提交就会因为wrong group不再提交
				kv.HistoryTran[cmd.Shard][cmd.ClientID] = tmp
			}
		case PUT:
			if kv.shardStatus[cmd.Shard] == RESPONSIBLE {
				kv.DataBase[cmd.Shard][cmd.Key] = cmd.Value
			} else {
				kv.HistoryTran[cmd.Shard][cmd.ClientID] = tmp
			}
		case APPEND:
			if kv.shardStatus[cmd.Shard] == RESPONSIBLE {
				kv.DataBase[cmd.Shard][cmd.Key] += cmd.Value
			} else {
				kv.HistoryTran[cmd.Shard][cmd.ClientID] = tmp
			}
		case SYNC:
			kv.shardStatus[cmd.Shard] = SENDSYNC
		case SYNCFIN:
			kv.shardStatus[cmd.Shard] = NOTRESPONSIBLE
			// delete(kv.DataBase, cmd.Shard)
			// delete(kv.HistoryTran, cmd.Shard)
			// kv.DataBase[cmd.Shard] = make(map[string]string)
			// kv.HistoryTran[cmd.Shard] = make(map[int64]int)
		case SYNCDB:
			for k, v := range cmd.DB {
				kv.DataBase[cmd.Shard][k] = v
			}
			for k, v := range cmd.HisTran {
				kv.HistoryTran[cmd.Shard][k] = v
			}
			kv.HistoryTran[cmd.Shard][cmd.ClientID] = cmd.TranscationID
			kv.shardStatus[cmd.Shard] = RESPONSIBLE
		case START:
			kv.shardStatus[cmd.Shard] = RESPONSIBLE
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
	DPrintf("me:%v read snapshot\n", kv.me)

	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("me:%v no snapshot\n", kv.me)
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
	kv.shardSendto = make([]int, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.DataBase[i] = make(map[string]string)
		kv.HistoryTran[i] = make(map[int64]int)
		kv.shardStatus[i] = NOTRESPONSIBLE
		kv.shardSendto[i] = -1
	}
	kv.CurCommitIndex = 0
	kv.isLeader = false
	kv.CurTerm = 0
	kv.serverID = nrand()
	kv.transcationID = 0
	kv.allknowgroups = make(map[int][]string)

	kv.readSnapshot(persister.ReadSnapshot())
	// kv重启后，logs里面存储了原本的状态，进行更新就能够恢复。
	// 然而，重启后不能接受新的cl请求，只能由自己commit一条空log
	go kv.commitemptylog()
	go kv.applier(kv.applyCh)
	go kv.termgetter()
	go kv.cfggetter()
	go kv.stop() // 避免死锁
	return kv
}

func (kv *ShardKV) commitemptylog() {
	// 启动srv后1秒，提交一条空commit
	time.Sleep(1000 * time.Millisecond)
	command := Op{ClientID: kv.serverID}
}

func (kv *ShardKV) stop() {
	time.Sleep(20 * time.Second)
	DFatal("time out stop\n")
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

	// for {
	// 	kv.rwmu.RLock()
	// 	leader := kv.isLeader
	// 	kv.rwmu.RUnlock()
	// 	if leader {
	// 		break
	// 	} else {
	// 		time.Sleep(100 * time.Millisecond)
	// 	}
	// }
	time.Sleep(1000 * time.Millisecond)
	// 必须保证获取第一条log前，group里已经存在一个leader
	for !kv.killed() {
		// 初始化首先获取1号cfg，从而避免初始化错误的问题
		curconfig = kv.sm.Query(1)
		kv.rwmu.Lock()
		kv.CurConfig = curconfig
		lastnum = curconfig.Num
		// 更新group信息
		for k, v := range kv.CurConfig.Groups {
			if kv.allknowgroups[k] == nil {
				kv.allknowgroups[k] = make([]string, len(v))
				copy(kv.allknowgroups[k], v)
			}
		}
		DPrintf("me:%v-%v db%v\n histran%v\n shardstatus%v\n cfg%v\n", kv.gid, kv.me, kv.DataBase, kv.HistoryTran, kv.shardStatus, kv.CurConfig)

		kv.rwmu.Unlock()
		kv.checkShard(curconfig)
		if lastnum == 1 {
			break
		}
	}

	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		curconfig = kv.sm.Query(-1)
		if lastnum != curconfig.Num {
			kv.rwmu.Lock()
			kv.CurConfig = curconfig
			lastnum = curconfig.Num
			// 更新group信息
			for k, v := range kv.CurConfig.Groups {
				if kv.allknowgroups[k] == nil {
					kv.allknowgroups[k] = make([]string, len(v))
					copy(kv.allknowgroups[k], v)
				}
			}
			// DPrintf("me:%v-%v curconfig%v\n", kv.gid, kv.me, kv.CurConfig)
			kv.rwmu.Unlock()
		}
		kv.checkShard(curconfig)
		// kv.rwmu.RLock()
		// DPrintf("me:%v-%v db%v\n histran%v\n shardstatus%v\n cfg%v\n", kv.gid, kv.me, kv.DataBase, kv.HistoryTran, kv.shardStatus, kv.CurConfig)
		// kv.rwmu.RUnlock()
	}
}

// 思考如何在server之间传递信息
// db分层后，给trans也分层记录 server之间传输时发送相应的db和trans
// 在每次config更新后,检查是否改变shards的范围
func (kv *ShardKV) checkShard(cfg shardctrler.Config) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	for i := 0; i < shardctrler.NShards; i++ {
		// 这里需要根据的状态发送任务，而不能先看shard的状态
		if kv.shardStatus[i] == RESPONSIBLE {
			if cfg.Shards[i] != kv.gid {
				// 这里避免自己修改shardstatus，全部改动通过commit执行，确保一致性
				// kv.shardStatus[i] = SENDSYNCWAITAGREE
				kv.shardSendto[i] = kv.CurConfig.Shards[i]
				if kv.isLeader {
					go kv.startsendsyncagree(i, kv.shardSendto[i])
				}
			}
			// } else if kv.shardStatus[i] == SENDSYNCWAITAGREE {
			// 	// 避免失联于是检查是leader就发送信息
			// 	if kv.isLeader {
			// 		go kv.startsendsyncagree(i, kv.shardSendto[i])
			// 	}
		} else if kv.shardStatus[i] == SENDSYNC {
			if kv.isLeader {
				go kv.startsendsync(i, kv.shardSendto[i])
			}
			// } else if kv.shardStatus[i] == SENDSYNCWAITFIN {
			// 	if kv.isLeader {
			// 		go kv.startsendsyncfin(i)
			// 	}
		} else if kv.shardStatus[i] == NOTRESPONSIBLE {
			if cfg.Shards[i] == kv.gid {
				if kv.isLeader {
					// 自己为leader,向其他group询问是否拥有该shard的数据
					go kv.askifsyncdb(i, cfg)
				}
				// if kv.CurConfig.Num == 1 {
				// 	kv.shardStatus[i] = RESPONSIBLE
				// } else {
				// 	kv.shardStatus[i] = WAITSYNC
				// }
			}
		} else { // kv.shardStatus[i] == WAITSYNC, do nothing

		}
	}
}

func (kv *ShardKV) startsendsyncagree(shard int, sentto int) {
	// leader先向group内部进行同步。
	// 对于某个需要同步的shard，首先提交到raft层，让group内部达成一致
	// 提交的command只需要类型与shard编号即可
	// 同步时，已经在本机提交的trans保留执行，执行完成后再把db和记录发送出去
	kv.rwmu.Lock()
	if kv.shardStatus[shard] != RESPONSIBLE {
		kv.rwmu.Unlock()
		return
	}
	kv.transcationID++
	command := Op{ClientID: kv.serverID, TranscationID: kv.transcationID, Type: SYNC, Shard: shard, Sendto: sentto}
	kv.rwmu.Unlock()
	flag := true
	for flag {
		// 采用循环的模式执行，因为担心raft层恰好无leader导致command提交失败
		// kv层的server不知道raft层是否存在leader,所以必须要多次尝试
		DPrintf("Try Sync shard%v In Group%v-%v\n", shard, kv.gid, kv.me)
		NeedReTry, Err := kv.submitSync(&command) // 还是和client区分开用另一个函数写方便一些
		if NeedReTry == NO {
			break
		}
		switch Err {
		case OK:
			flag = false
		case ErrKilled:
			return
		case ErrTermchanged:
		case ErrWrongLeader:
			return
		case ErrSyncDBCompleted:
			return
		case ErrCompleted:
			flag = false
		case ErrNotcommit:
		}
		if Err == ErrKilled {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	go kv.startsendsync(shard, sentto)
}

func (kv *ShardKV) startsendsync(shard int, sentto int) {
	// 认为同步完成，开始向目标shard发送db和cl
	kv.rwmu.RLock()
	if kv.shardStatus[shard] != SENDSYNC {
		kv.rwmu.RUnlock()
		return
	}
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
	group := kv.CurConfig.Groups[sentto]
	kv.rwmu.RUnlock()

	// 在这一部分 3对3都发送syncdb,目标是让对方至少一半成功同步db
	// 在对方返回成功信息后,需要把成功信息也至少发送给一半原server
	// 有可能出现返回的leader被屏蔽后丢失command，导致原group一直处于send状态
	flag := true
	for flag {
		for si := 0; si < len(group); si++ {
			srv := kv.make_end(group[si])
			ok := srv.Call("ShardKV.SyncDB", &args, &reply)
			if !ok || reply.Err == ErrKilled || reply.Err == ErrWrongLeader {
				continue
			}
			if reply.Err == OK {
				DPrintf("%v-%v send%v to %v-%v succeed\n", kv.gid, kv.me, shard, sentto, si)
				flag = false
				break
			}
			if reply.Err == ErrCompleted {
				// fmt.Printf("cl%v %v to src%v already\n", ck.clientID, op, curserver)
				flag = false
				break
			}
			if reply.Err == ErrWrongGroup {
				return
			}
			if reply.Err == ErrNoKey || reply.Err == ErrTermchanged {
				// fmt.Printf("cl%v %v to srv%v failed:%v\n", ck.clientID, op, curserver, reply.Err)
				continue
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	go kv.startsendsyncfin(shard)
}

func (kv *ShardKV) startsendsyncfin(shard int) {
	// 收到成功的消息，对自己的server进行同步完成的消息
	kv.rwmu.Lock()
	if kv.shardStatus[shard] != SENDSYNC {
		kv.rwmu.Unlock()
		return
	}
	kv.transcationID++
	command := Op{ClientID: kv.serverID, TranscationID: kv.transcationID, Type: SYNCFIN, Shard: shard}
	kv.rwmu.Unlock()
	flag := true
	for flag {
		// 采用循环的模式执行，因为担心raft层恰好无leader导致command提交失败
		// kv层的server不知道raft层是否存在leader,所以必须要多次尝试
		DPrintf("Try Syncfin shard%v In Group%v-%v\n", shard, kv.gid, kv.me)
		NeedReTry, Err := kv.submitSync(&command) // 还是和client区分开用另一个函数写方便一些
		if NeedReTry == NO {
			break
		}
		switch Err {
		case OK:
			flag = false
		case ErrKilled:
			return
		case ErrTermchanged:
		case ErrWrongLeader:
			return
		case ErrSyncDBCompleted:
			return
		case ErrCompleted:
			flag = false
		case ErrNotcommit:
		}
		if Err == ErrKilled {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) submitSync(cmd *Op) (string, Err) {
	// 大体模仿submitcmd的函数逻辑,简单修改返回的内容
	needretry := YES
	err := Err(OK)
	kv.rwmu.RLock()
	if kv.shardStatus[cmd.Shard] != RESPONSIBLE && cmd.Type == SYNC {
		err = ErrCompleted
		needretry = NO
		kv.rwmu.RUnlock()
		return needretry, err
	} else if kv.shardStatus[cmd.Shard] != SENDSYNC && cmd.Type == SYNCFIN {
		err = ErrCompleted
		needretry = NO
		kv.rwmu.RUnlock()
		return needretry, err
	} else if kv.shardStatus[cmd.Shard] != NOTRESPONSIBLE && cmd.Type == START {
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
				} else if kv.shardStatus[cmd.Shard] != RESPONSIBLE && cmd.Type == START {
					err = ErrCompleted
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
	fmt.Printf("shardstatus%v\n", kv.shardStatus)
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

func (kv *ShardKV) askifsyncdb(shard int, cfg shardctrler.Config) {
	// leader向已知的group进行询问，问其是否拥有该shard
	// 每个group检查自己的shardstatus，如果是responsible就检查cfg是否正确
	// 如果不正确，则启动sendsync
	// 如果处于sendsync，则告知请求方等待
	// 请求方如果发现所有group都不拥有shard，再次检查自己是否拥有shard权限
	// 启动commit流程，将自己转换为responsible
	args := AskDBArgs{kv.serverID, shard}
	reply := AskDBReply{}
	kv.rwmu.RLock()
	// 深拷贝？
	groups := make(map[int][]string)
	for k, v := range kv.allknowgroups {
		newSlice := make([]string, len(v))
		copy(newSlice, v) // 深拷贝 slice
		groups[k] = newSlice
	}
	kv.rwmu.RUnlock()

	for gid, servers := range groups {
		if gid == kv.gid {
			continue // 跳过自己
		}
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			DPrintf("%v-%v-%v ask %v-%v,args%v\n", kv.gid, kv.me, shard, gid, si, args)
			ok := srv.Call("ShardKV.AskDB", &args, &reply)
			if !ok || reply.Err == ErrKilled || reply.Err == ErrWrongLeader {
				continue
			}
			if reply.Err == ErrHoldDB || reply.Err == ErrSendingDB {
				DPrintf("%v-%v-%v:%v-%v is %v\n", kv.gid, kv.me, shard, gid, si, reply.Err)
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 收到全部group都没有，内部发送submit流程，把自己转变为responsible
	kv.rwmu.Lock()
	if kv.shardStatus[shard] != NOTRESPONSIBLE || cfg.Num != 1 {
		kv.rwmu.Unlock()
		return
	}
	kv.transcationID++
	command := Op{ClientID: kv.serverID, TranscationID: kv.transcationID, Type: START, Shard: shard}
	kv.rwmu.Unlock()
	flag := true
	for flag {
		// 采用循环的模式执行，因为担心raft层恰好无leader导致command提交失败
		// kv层的server不知道raft层是否存在leader,所以必须要多次尝试
		DPrintf("Try Start shard%v In Group%v-%v\n", shard, kv.gid, kv.me)
		NeedReTry, Err := kv.submitSync(&command) // 还是和client区分开用另一个函数写方便一些
		if NeedReTry == NO {
			break
		}
		switch Err {
		case OK:
			flag = false
		case ErrKilled:
			return
		case ErrTermchanged:
		case ErrWrongLeader:
			return
		case ErrSyncDBCompleted:
			return
		case ErrCompleted:
			flag = false
		case ErrNotcommit:
		}
		if Err == ErrKilled {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) AskDB(args *AskDBArgs, reply *AskDBReply) {
	reply.ServerID = kv.serverID
	kv.rwmu.RLock()
	defer kv.rwmu.RUnlock()
	if !kv.isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.shardStatus[args.Shard] == NOTRESPONSIBLE {
		reply.Err = ErrWrongGroup
	} else if kv.shardStatus[args.Shard] == SENDSYNC {
		reply.Err = ErrSendingDB
	} else if kv.shardStatus[args.Shard] == RESPONSIBLE {
		if kv.CurConfig.Shards[args.Shard] == kv.gid {
			reply.Err = ErrHoldDB
		} else {
			go kv.startsendsyncagree(args.Shard, kv.CurConfig.Shards[args.Shard])
			reply.Err = ErrSendingDB
		}
	}
	fmt.Printf("%v-%v-%v reply%v\n", kv.gid, kv.me, args.Shard, kv.shardStatus)
}
