package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	serverNum     int
	clientID      int64
	transcationID int
	leaderID      int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.serverNum = len(servers)
	ck.clientID = nrand()
	ck.transcationID = 0
	ck.leaderID = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.transcationID++
	args := QueryArgs{ck.clientID, ck.transcationID, QUERY, num}

	reply := QueryReply{}
	// Your code here.
	ck.CallServer(&args, &reply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) CallServer(args Args, reply Reply) {
	leader := ck.leaderID
	curserver := leader
	op := args.getType()
	for {
		// fmt.Printf("cl%v %v to srv%v\n", ck.clientID, op, curserver)
		ok := ck.servers[curserver].Call("KVServer."+op, args, reply)
		if !ok || reply.getErr() == ErrKilled || reply.getErr() == ErrWrongLeader {
			// fmt.Printf("cl%v %v to srv%v failed:%v\n", ck.clientID, op, curserver, reply.Err)
			curserver = (curserver + 1) % ck.serverNum
			if curserver == leader {
				// fmt.Printf("cl%v %v but no leader, wait 0.3s\n", ck.clientID, op)
				time.Sleep(300 * time.Millisecond)
			}
			continue
		}
		if reply.getErr() == OK {
			DPrintf("cl%v %v to srv%v succeed\n", ck.clientID, op, curserver)
			break
		}
		if reply.getErr() == ErrCompleted {
			// fmt.Printf("cl%v %v to src%v already\n", ck.clientID, op, curserver)
			break
		}
		if reply.getErr() == ErrNoKey || reply.getErr() == ErrTermchanged {
			// fmt.Printf("cl%v %v to srv%v failed:%v\n", ck.clientID, op, curserver, reply.Err)
			continue
		}
	}
	if leader != curserver {
		ck.leaderID = curserver
		// fmt.Printf("cl%v update leaderID to %v\n", ck.clientID, ck.leaderID)
	}
}
