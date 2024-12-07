package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

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
	// You'll have to add code here.
	ck.serverNum = len(servers)
	ck.clientID = nrand()
	ck.transcationID = 0
	ck.leaderID = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.transcationID++
	args := KVArgs{ck.clientID, ck.transcationID, GET, key, ""}
	reply := KVReply{}
	// args.Print()
	ck.CallServer(&args, &reply)
	// fmt.Printf("cl%v %v complete\n", ck.clientID, GET)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.transcationID++
	args := KVArgs{ck.clientID, ck.transcationID, op, key, value}
	reply := KVReply{}
	// args.Print()
	ck.CallServer(&args, &reply)
	// fmt.Printf("cl%v %v complete\n", ck.clientID, op)
	// if op == APPEND {
	// 	ck.Report()
	// }
}

// func (ck *Clerk) Report() {
// 	ck.transcationID++
// 	args := KVArgs{ck.clientID, ck.transcationID, REPORT, "", ""}
// 	reply := KVReply{}
// 	// args.Print()
// 	ck.CallServer(&args, &reply)
// 	fmt.Printf("cl%v %v complete\n", ck.clientID, REPORT)
// }

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) CallServer(args *KVArgs, reply *KVReply) {
	leader := ck.leaderID
	curserver := leader
	op := args.Type
	for {
		// fmt.Printf("cl%v %v to srv%v\n", ck.clientID, op, curserver)
		ok := ck.servers[curserver].Call("KVServer."+op, args, reply)
		if !ok || reply.Err == ErrKilled || reply.Err == ErrWrongLeader {
			// fmt.Printf("cl%v %v to srv%v failed:%v\n", ck.clientID, op, curserver, reply.Err)
			curserver = (curserver + 1) % ck.serverNum
			if curserver == leader {
				// fmt.Printf("cl%v %v but no leader, wait 0.3s\n", ck.clientID, op)
				time.Sleep(300 * time.Millisecond)
			}
			continue
		}
		if reply.Err == OK {
			fmt.Printf("cl%v %v to srv%v succeed\n", ck.clientID, op, curserver)
			break
		}
		if reply.Err == ErrCompleted {
			// fmt.Printf("cl%v %v to src%v already\n", ck.clientID, op, curserver)
			break
		}
		if reply.Err == ErrNoKey || reply.Err == ErrTermchanged {
			// fmt.Printf("cl%v %v to srv%v failed:%v\n", ck.clientID, op, curserver, reply.Err)
			continue
		}
	}
	if leader != curserver {
		ck.leaderID = curserver
		// fmt.Printf("cl%v update leaderID to %v\n", ck.clientID, ck.leaderID)
	}
}
