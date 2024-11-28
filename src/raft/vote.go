package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Me   int
	Term int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Me   int
	Vote bool
}

type HeartbeatsArgs struct {
	Me   int
	Term int
}

type HeartbeatsReply struct {
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	reply.Me = rf.me
	if rf.voteTo != -1 && args.Term <= rf.term {
		fmt.Printf("me:%v refuse vote %v\n", rf.me, args.Me)
		reply.Vote = false
	} else {
		fmt.Printf("me:%v vote %v\n", rf.me, args.Me)
		reply.Vote = true
		rf.mu.Lock()
		rf.state = StateFollower
		rf.voteTo = args.Me
		// 注意锁的获取释放,因为go不会主动提示锁未释放
		rf.mu.Unlock()
		rf.heartbeatTimer.Stop()
		rf.heartbeatTimer.Reset(randomVoteTimeout())
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// fmt.Printf("me:%v state%v\n", rf.me, rf.state)
		switch rf.state {
		case StateFollower:
			rf.runFollower()
		case StateCandidate:
			rf.runCandidate()
		case StateLeader:
			rf.runLeader()
		}

	}
}

func (rf *Raft) runFollower() {
	fmt.Printf("me:%v is follower\n", rf.me)

	<-rf.heartbeatTimer.C
	fmt.Printf("me:%v heartbeater timeout\n", rf.me)
	rf.mu.Lock()
	rf.state = StateCandidate
	rf.mu.Unlock()
}

func (rf *Raft) runCandidate() {
	fmt.Printf("me:%v is candidate\n", rf.me)
	rf.mu.Lock()
	rf.term++
	fmt.Printf("me:%v term%v\n", rf.me, rf.term)
	rf.voteTo = rf.me
	rf.voteBox[rf.me] = 1
	fmt.Printf("me:%v vote to %v\n", rf.me, rf.voteTo)
	rf.mu.Unlock()

	// start vote
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.startVote(i)
	}
	rf.resetVoteTimer()

	<-rf.voteTimer.C
	fmt.Printf("me:%v vote time out, check if leader\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == StateFollower {
		fmt.Printf("me:%v become follower\n", rf.me)
		return
	}
	sum := 0
	for _, num := range rf.voteBox {
		sum += num
	}
	if sum > len(rf.peers)/2 {
		fmt.Printf("me:%v become leader\n", rf.me)
		rf.state = StateLeader
		for i := range rf.voteBox {
			rf.voteBox[i] = 0
		}
		rf.voteTo = -1
	} else {
		fmt.Printf("me:%v vote failed\n", rf.me)
		rf.state = StateFollower
		for i := range rf.voteBox {
			rf.voteBox[i] = 0
		}
		rf.voteTo = -1
	}
}

func (rf *Raft) startVote(server int) {
	args := RequestVoteArgs{rf.me, rf.term}
	reply := RequestVoteReply{}
	ok := false
	for !ok {
		ok = rf.sendRequestVote(server, &args, &reply)
		time.Sleep(10 * time.Millisecond)
	}
	if reply.Vote {
		rf.mu.Lock()
		rf.voteBox[server] = 1
		rf.mu.Unlock()
	}
}

func (rf *Raft) runLeader() {
	fmt.Printf("me:%v is leader\n", rf.me)
	// start vote
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.startHeartBeat(i)
	}
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) startHeartBeat(server int) {
	args := HeartbeatsArgs{rf.me, rf.term}
	reply := HeartbeatsReply{}
	rf.peers[server].Call("Raft.HeartBeat", &args, &reply)
}

func (rf *Raft) HeartBeat(args *HeartbeatsArgs, reply *HeartbeatsReply) {
	if rf.term > args.Term {
		fmt.Printf("%v is old term leader, ignore\n", args.Me)
	} else {
		// fmt.Printf("me:%v receive heartbeat\n", rf.me)
		rf.mu.Lock()
		rf.heartbeatTimer.Reset(randomHeartbeatTimeout())
		rf.state = StateFollower
		rf.term = args.Term
		rf.voteTo = -1
		for i := range rf.voteBox {
			rf.voteBox[i] = 0
		}
		rf.mu.Unlock()
	}
}
