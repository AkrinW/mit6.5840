package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
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
	ck.CallServer(&args, &reply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.transcationID++
	args := JoinArgs{ck.clientID, ck.transcationID, JOIN, servers}
	reply := JoinReply{}
	ck.CallServer(&args, &reply)
}

func (ck *Clerk) Leave(gids []int) {
	ck.transcationID++
	args := LeaveArgs{ck.clientID, ck.transcationID, LEAVE, gids}
	reply := LeaveReply{}
	ck.CallServer(&args, &reply)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.transcationID++
	args := MoveArgs{ck.clientID, ck.transcationID, MOVE, shard, gid}
	reply := MoveReply{}
	ck.CallServer(&args, &reply)
}

func (ck *Clerk) CallServer(args Args, reply Reply) {
	leader := ck.leaderID
	curserver := leader
	op := args.getType()
	for {
		fmt.Printf("cl%v %v to srv%v\n", ck.clientID, op, curserver)
		ok := ck.servers[curserver].Call("ShardCtrler."+op, args, reply)
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
