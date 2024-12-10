package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID      int64
	transcationID int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.transcationID = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.transcationID++
	args := KVArgs{ClientID: ck.clientID, TranscationID: ck.transcationID, Type: GET, Key: key}
	reply := KVReply{}
	ck.CallServer(&args, &reply)
	return reply.Value
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.transcationID++
	args := KVArgs{ClientID: ck.clientID, TranscationID: ck.transcationID, Type: op, Key: key, Value: value}
	reply := KVReply{}
	ck.CallServer(&args, &reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) CallServer(args *KVArgs, reply *KVReply) {
	shard := key2shard(args.Key)
	args.Shard = shard
	op := args.Type
	for {
		gid := ck.config.Shards[shard]
		if server, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(server); si++ {
				srv := ck.make_end(server[si])
				ok := srv.Call("ShardKV."+op, args, reply)
				if !ok || reply.Err == ErrKilled || reply.Err == ErrWrongLeader {
					continue
				}
				if reply.Err == OK {
					DPrintf("cl%v %v to srv%v succeed\n", ck.clientID, op, server[si])
					return
				}
				if reply.Err == ErrCompleted {
					// fmt.Printf("cl%v %v to src%v already\n", ck.clientID, op, curserver)
					return
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
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
