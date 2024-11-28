package raft

import (
	"fmt"
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
	Term int // 这个用于提示投票是哪次任期的
}

type HeartbeatsArgs struct {
	Me   int
	Term int
}

type HeartbeatsReply struct {
	Term int // 返回自己的term给leader，如果leader发现自己落后了，就把自己变为follower
}

// 需要注意数据争用的问题，比较关键的数据类型每次读或写都需要加锁，保证原子性

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	reply.Me = rf.me
	reply.Term = args.Term
	rf.mu.Lock()
	if args.Term > rf.term || (rf.voteTo == -1 && args.Term == rf.term) {
		rf.state = StateFollower
		rf.term = args.Term
		rf.voteTo = args.Me
		rf.heartbeatTimer.Stop()
		rf.heartbeatTimer.Reset(randomVoteTimeout())
		rf.mu.Unlock()
		fmt.Printf("me:%v vote %v, term%v\n", rf.me, args.Me, reply.Term)
		reply.Vote = true
		// 注意锁的获取释放,因为go不会主动提示锁未释放
	} else {
		rf.mu.Unlock()
		fmt.Printf("me:%v refuse vote %v, term%v\n", rf.me, args.Me, reply.Term)
		reply.Vote = false
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
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		// fmt.Printf("me:%v state%v\n", rf.me, rf.state)
		rf.mu.Lock()
		switch rf.state {
		case StateFollower:
			rf.mu.Unlock()
			rf.runFollower()
		case StateCandidate:
			rf.mu.Unlock()
			rf.runCandidate()
		case StateLeader:
			rf.mu.Unlock()
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
	// 开始投票，初始化自己的选票状态
	rf.term++
	fmt.Printf("me:%v term%v\n", rf.me, rf.term)
	rf.voteTo = rf.me
	rf.voteGets = 1
	rf.mu.Unlock()

	rf.resetVoteTimer()
	// start vote
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.startVote(i)
	}

	<-rf.voteTimer.C
	fmt.Printf("me:%v vote time out, check if leader\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == StateFollower {
		fmt.Printf("me:%v become follower\n", rf.me)
		return
	}
	if rf.voteGets > len(rf.peers)/2 {
		fmt.Printf("me:%v become leader\n", rf.me)
		rf.state = StateLeader
	} else {
		fmt.Printf("me:%v vote failed\n", rf.me)
		rf.state = StateFollower
		rf.restHeartbeatTimer()
		rf.voteTo = -1
	}
}

func (rf *Raft) startVote(server int) {
	rf.mu.Lock()
	args := RequestVoteArgs{rf.me, rf.term}
	rf.mu.Unlock()
	reply := RequestVoteReply{}
	ok := false
	for !ok {
		ok = rf.sendRequestVote(server, &args, &reply)
		time.Sleep(10 * time.Millisecond)
	}
	// 检查是不是无效票
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == StateCandidate && reply.Vote && reply.Term == rf.term {
		rf.voteGets++
		// 即时检查选票，如果当选立刻变成leader
		if rf.voteGets > len(rf.peers)/2 {
			rf.voteTimer.Reset(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) runLeader() {
	fmt.Printf("me:%v is leader\n", rf.me)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.startHeartBeat(i)
	}
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) startHeartBeat(server int) {

	rf.mu.Lock()
	args := HeartbeatsArgs{rf.me, rf.term}
	rf.mu.Unlock()
	reply := HeartbeatsReply{}
	rf.peers[server].Call("Raft.HeartBeat", &args, &reply)

	rf.mu.Lock()
	if reply.Term > rf.term {
		rf.state = StateFollower
		rf.term = reply.Term
	}
	rf.mu.Unlock()
}

func (rf *Raft) HeartBeat(args *HeartbeatsArgs, reply *HeartbeatsReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term {
		fmt.Printf("%v is old term leader, ignore\n", args.Me)
		reply.Term = rf.term
	} else {
		// fmt.Printf("me:%v receive heartbeat\n", rf.me)
		rf.heartbeatTimer.Reset(randomHeartbeatTimeout())
		rf.state = StateFollower
		rf.term = args.Term
	}
}
