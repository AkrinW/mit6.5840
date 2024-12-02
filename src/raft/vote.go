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
	// 投票时发送自己已提交的index和log长度。
	CommitIndex int
	Index       int
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
	Me          int
	Term        int
	CommitIndex int //心跳发送leader已commit节点
	CommitTerm  int
	CurIndex    int //心跳发送当前最新节点
	CurTerm     int
}

type HeartbeatsReply struct {
	Me          int
	Term        int  // 返回自己的term给leader，如果leader发现自己落后了，就把自己变为follower
	CommitIndex int  // 返回自己的commitindex，给leader检查
	IfNeedMatch bool // 返回自己是否需要同步
}

// 需要注意数据争用的问题，比较关键的数据类型每次读或写都需要加锁，保证原子性
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
		state := rf.state
		rf.mu.Unlock()
		switch state {
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
	// fmt.Printf("me:%v is follower\n", rf.me)

	<-rf.heartbeatTimer.C
	if rf.killed() {
		return
	}
	fmt.Printf("me:%v heartbeater timeout\n", rf.me)
	rf.mu.Lock()
	rf.state = StateCandidate
	// 开始投票，初始化自己的选票状态
	rf.term++
	fmt.Printf("me:%v term%v\n", rf.me, rf.term)
	rf.voteTo = rf.me
	rf.voteGets = 1
	rf.ifstopvote = false
	rf.persist()
	rf.mu.Unlock()

	ResetTimer(rf.voteTimer, 500, 500)
	// start vote
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.startVote(i)
	}
}

func (rf *Raft) runCandidate() {
	// fmt.Printf("me:%v is candidate\n", rf.me)

	// 将candidate的选票逻辑移动到follower了，目的是为了保持term变化的原子性
	// rf.mu.Lock()
	// // 开始投票，初始化自己的选票状态
	// rf.term++
	// fmt.Printf("me:%v term%v\n", rf.me, rf.term)
	// rf.voteTo = rf.me
	// rf.voteGets = 1
	// rf.ifstopvote = false
	// rf.persist()
	// rf.mu.Unlock()

	// ResetTimer(rf.voteTimer, 500, 500)
	// // start vote
	// for i := 0; i < rf.serverNum; i++ {
	// 	if i == rf.me {
	// 		continue
	// 	}
	// 	go rf.startVote(i)
	// }

	<-rf.voteTimer.C
	if rf.killed() {
		return
	}
	fmt.Printf("me:%v vote time out, check if leader\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ifstopvote = false
	if rf.state == StateFollower {
		fmt.Printf("me:%v become follower\n", rf.me)
		return
	}
	if rf.voteGets > rf.serverNum/2 {
		fmt.Printf("me:%v become leader\n", rf.me)
		rf.state = StateLeader
		// 变为leader后，启动一个goroutine检查是否进行commit
		// 这个routine会在rf变为follower后停止
		go rf.commiter()
	} else {
		fmt.Printf("me:%v vote failed\n", rf.me)
		rf.state = StateFollower
		ResetTimer(rf.heartbeatTimer, 200, 150)
		rf.voteTo = -1
	}
	rf.persist()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	reply.Me = rf.me
	reply.Term = args.Term
	rf.mu.Lock()
	// 重新修改投票逻辑，先看任期，再看已commit长度，再看log长度，最后看是否已经投票
	if args.Term < rf.term {
		rf.mu.Unlock()
		reply.Vote = false
		fmt.Printf("me:%v refuse vote %v, term%v, old term\n", rf.me, args.Me, reply.Term)
		return
	}
	if args.CommitIndex < rf.commitIndex {
		if rf.term != args.Term {
			rf.term = args.Term
			rf.persist()
		}
		rf.mu.Unlock()
		reply.Vote = false
		fmt.Printf("me:%v refuse vote %v, term%v, old commit\n", rf.me, args.Me, reply.Term)
	} else if args.CommitIndex == rf.commitIndex && args.Index < rf.nextIndex {
		if rf.term != args.Term {
			rf.term = args.Term
			rf.persist()
		}
		rf.mu.Unlock()
		reply.Vote = false
		fmt.Printf("me:%v refuse vote %v, term%v, old log index\n", rf.me, args.Me, reply.Term)
	} else {
		if rf.voteTo != -1 && args.Term == rf.term {
			rf.mu.Unlock()
			reply.Vote = false
			fmt.Printf("me:%v in term%v already voted\n", rf.me, reply.Term)
		} else {
			rf.state = StateFollower
			rf.term = args.Term
			rf.voteTo = args.Me
			ResetTimer(rf.heartbeatTimer, 300, 150)
			rf.persist()
			rf.mu.Unlock()
			fmt.Printf("me:%v vote %v, term%v\n", rf.me, args.Me, reply.Term)
			reply.Vote = true
		}
	}
	// if args.Term > rf.term || (rf.voteTo == -1 && args.Term == rf.term) {
	// 	rf.state = StateFollower
	// 	rf.term = args.Term
	// 	rf.voteTo = args.Me
	// 	ResetTimer(rf.heartbeatTimer, 500, 500)
	// 	rf.mu.Unlock()
	// 	fmt.Printf("me:%v vote %v, term%v\n", rf.me, args.Me, reply.Term)
	// 	reply.Vote = true
	// 	// 注意锁的获取释放,因为go不会主动提示锁未释放
	// } else {
	// 	rf.mu.Unlock()
	// 	fmt.Printf("me:%v refuse vote %v, term%v\n", rf.me, args.Me, reply.Term)
	// 	reply.Vote = false
	// }
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

func (rf *Raft) startVote(server int) {
	rf.mu.Lock()
	args := RequestVoteArgs{rf.me, rf.term, rf.commitIndex, rf.nextIndex}
	rf.mu.Unlock()
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		// send失败在test中说明对方断连了，直接中止代码
		return
		// ok = rf.sendRequestVote(server, &args, &reply)
		// time.Sleep(50 * time.Millisecond)
	}
	// 检查是不是无效票
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == StateCandidate && reply.Vote && reply.Term == rf.term {
		rf.voteGets++
		// 即时检查选票，如果当选立刻变成leader
		if rf.voteGets > rf.serverNum/2 && !rf.ifstopvote {
			rf.voteTimer.Reset(10 * time.Microsecond)
			rf.ifstopvote = true
		}
	}
}

func (rf *Raft) runLeader() {
	fmt.Printf("me:%v is leader\n", rf.me)
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.startHeartBeat(i)
	}
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) startHeartBeat(server int) {
	rf.mu.Lock()
	// 这里修改 因为存在锁释放又获取的情况，nextindex可能在这段时间改变
	// 所以为了避免错误修改，只获取函数执行时的index，并在后面检查是否需要修改
	curIndex := rf.nextIndex - 1
	args := HeartbeatsArgs{rf.me, rf.term, rf.commitIndex, rf.logs[rf.commitIndex].Term, curIndex, rf.logs[curIndex].Term}
	rf.mu.Unlock()
	reply := HeartbeatsReply{}
	// leader向follower发送自己的commit与logindex，让follower知道自己是否落后
	ok := rf.peers[server].Call("Raft.HeartBeat", &args, &reply)
	if !ok {
		// fmt.Printf("server%v unconnect, return\n", server)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.term || reply.CommitIndex > rf.commitIndex {
		rf.state = StateFollower
		ResetTimer(rf.heartbeatTimer, 200, 150)
		rf.term = reply.Term
		rf.persist()
		return
	}
	// 检查reply的commit，如果落后，就让它进行更新
	if reply.IfNeedMatch {
		if !rf.incheck[server] {
			go rf.MatchLog(server)
		}
	} else {
		if rf.matchIndex[server] < curIndex {
			fmt.Printf("change matchindex[%v]in startheartbeat\n", server)
			rf.matchIndex[server] = curIndex
		}
	}
	// if reply.CommitIndex < rf.commitIndex {
	// 	go rf.HeartCommitLog(server, reply.CommitIndex, rf.commitIndex)
	// } else if rf.matchIndex[server] < rf.nextIndex-1 {
	// 	// 要求follower先完成之前的commit，再进行logentry复制
	// 	go rf.MatchLog(server)
	// }
}

func (rf *Raft) HeartBeat(args *HeartbeatsArgs, reply *HeartbeatsReply) {
	reply.Me = rf.me
	reply.IfNeedMatch = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term {
		fmt.Printf("%v is old term leader, ignore\n", args.Me)
		reply.Term = rf.term
	} else if args.CommitIndex < rf.commitIndex {
		// 需要检查leader是否有更新的commit，如果落后，就不能接受heartbeat
		fmt.Printf("%v is old commit leader, ignore\n", args.Me)
		reply.CommitIndex = rf.commitIndex
		if rf.term < args.Term {
			rf.state = StateFollower
			rf.term = args.Term
			ResetTimer(rf.heartbeatTimer, 200, 150)
			rf.persist()
		}
	} else {
		// fmt.Printf("me:%v receive heartbeat\n", rf.me)
		ResetTimer(rf.heartbeatTimer, 200, 150)
		if rf.state != StateFollower || rf.term != args.Term {
			rf.state = StateFollower
			rf.term = args.Term
			rf.persist()
		}
		if args.CurIndex > rf.nextIndex-1 || args.CurTerm != rf.logs[args.CurIndex].Term {
			reply.IfNeedMatch = true
		}
		if args.CommitIndex < rf.nextIndex && args.CommitTerm == rf.logs[args.CommitIndex].Term {
			for rf.commitIndex < args.CommitIndex {
				rf.commitIndex++
				fmt.Printf("me:%v commit log[%v]\n", rf.me, rf.commitIndex)
				msg := ApplyMsg{CommandValid: true, CommandIndex: rf.commitIndex, Command: rf.logs[rf.commitIndex].Command}
				rf.applyCh <- msg
			}
		}
		reply.CommitIndex = rf.commitIndex
	}
}
