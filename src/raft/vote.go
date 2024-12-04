package raft

import (
	"fmt"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Me   int
	Term int
	// 投票时发送自己已提交的index和log长度。
	CommitIndex int
	CurIndex    int
	CurTerm     int // 添加最新的log的term，在commit相同时，保证有最新term的当选
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Me         int
	Vote       bool
	Term       int  // 返回自己的term，帮助candidate快速更新
	IfOutedate bool // 用来提醒发送者是否过时了
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
	Term        int // 返回自己的term给leader，如果leader发现自己落后了，就把自己变为follower
	CommitIndex int // 返回自己的commitindex，给leader检查
	// IfNeedMatch bool             // 返回自己是否需要同步
	SLogEntries []SimpleLogEntry //返回从commit开始的所有log，用于比较一致性
	Start       int              // 用来提示slog的开头是哪个index
	IfOutedate  bool
}

// 重写思路，用读写锁的形式，在函数开头用读锁获取全部数据
// 需要修改的部分放在结尾，用写锁一并更新

// 需要注意数据争用的问题，比较关键的数据类型每次读或写都需要加锁，保证原子性
func (rf *Raft) ticker() {
	ResetTimer(rf.heartbeatTimer, 200, 150)
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		// fmt.Printf("me:%v state%v\n", rf.me, rf.state)

		rf.rwmu.RLock()
		state := rf.state
		rf.rwmu.RUnlock()

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

	rf.rwmu.Lock()
	rf.state = StateCandidate
	// 开始投票，初始化自己的选票状态
	rf.term++
	fmt.Printf("me:%v term%v\n", rf.me, rf.term)
	rf.voteTo[rf.term] = rf.me
	rf.voteGets[rf.term] = 1
	rf.voteDisagree[rf.term] = 0
	rf.ifstopvote = false
	rf.persist()
	ResetTimer(rf.voteTimer, 400, 400) // 时间重置需要和锁一起

	rf.rwmu.Unlock()

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

	<-rf.voteTimer.C
	if rf.killed() {
		return
	}
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	fmt.Printf("me:%v vote time out, check if leader\n", rf.me)
	rf.ifstopvote = true
	if rf.state == StateFollower {
		fmt.Printf("me:%v become follower\n", rf.me)
		ResetTimer(rf.heartbeatTimer, 200, 150)
		return
	}
	if rf.voteGets[rf.term] > rf.serverNum/2 {
		fmt.Printf("me:%v term%v become leader\n", rf.me, rf.term)
		rf.state = StateLeader
		// 变为leader后，启动一个goroutine检查是否进行commit
		// 这个routine会在rf变为follower后停止
		// 每次成为leader后，需要重新构建matchindex，把自己外的全部置0
		for i := 0; i < rf.serverNum; i++ {
			rf.matchIndex[i] = 0
			if i == rf.me {
				rf.matchIndex[i] = rf.nextIndex - 1
			}
		}
		// go rf.commiter()
	} else {
		fmt.Printf("me:%v term%v vote failed\n", rf.me, rf.term)
		rf.state = StateFollower
		ResetTimer(rf.heartbeatTimer, 200, 150)
	}
	rf.persist()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	reply.Me = rf.me
	// 重新修改投票逻辑，先看任期，再看已commit长度，再看log长度，最后看是否已经投票
	// 这个投票逻辑是错误的。。。按照论文要求，只需要比较最新的index的term和index即可
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	term := rf.term
	commitindex := rf.commitIndex
	curindex := rf.nextIndex - 1
	offset := rf.snapoffset
	// 所有rf.logs[]的操作都减去一个offset
	curterm := rf.logs[curindex-offset].Term
	_, exists := rf.voteTo[args.Term]

	reply.Term = term
	reply.Vote = false
	if args.Term < term {
		reply.IfOutedate = true
		// fmt.Printf("me:%v refuse vote %v, term%v, old term\n", rf.me, args.Me, reply.Term)
		return
	} else if args.Term == term {
		// 同一任期
		if !exists {
			if args.CurTerm > curterm {
				reply.Vote = true
			}
			if args.CurTerm == curterm {
				if args.CurIndex > curindex {
					reply.Vote = true
				}
				if args.CurIndex == curindex && args.CommitIndex >= commitindex {
					reply.Vote = true
				}
			} // 和注释里的原投票逻辑更改了一下顺序。。希望能完成测试

			// if args.CommitIndex > commitindex {
			// 	reply.Vote = true
			// }
			// if args.CommitIndex == commitindex {
			// 	if args.CurTerm > curterm {
			// 		reply.Vote = true
			// 	}
			// 	if args.CurTerm == curterm && args.CurIndex >= curindex {
			// 		reply.Vote = true
			// 	}
			// }
		}
	} else {
		if args.CurTerm > curterm {
			reply.Vote = true
		}
		if args.CurTerm == curterm {
			if args.CurIndex > curindex {
				reply.Vote = true
			}
			if args.CurIndex == curindex && args.CommitIndex >= commitindex {
				reply.Vote = true
			}
		}
		rf.term = args.Term
		rf.persist()
	}

	if reply.Vote {
		ResetTimer(rf.heartbeatTimer, 300, 150)
		rf.voteTo[args.Term] = args.Me
		if rf.state != StateFollower {
			rf.state = StateFollower
			ResetTimer(rf.voteTimer, 1, 1)
			ResetTimer(rf.leaderTimer, 1, 1)
		}
		fmt.Printf("me:%v vote %v, term%v\n", rf.me, args.Me, reply.Term)
		rf.persist()
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

func (rf *Raft) startVote(server int) {
	rf.rwmu.RLock()
	term := rf.term
	args := RequestVoteArgs{rf.me, term, rf.commitIndex, rf.nextIndex - 1, rf.logs[rf.nextIndex-1-rf.snapoffset].Term}
	reply := RequestVoteReply{}
	rf.rwmu.RUnlock()
	ok := false
	rpccount := 0
	for !ok {
		// send失败在test中说明对方断连了，直接中止代码
		// TestUnreliable3C后，正常rpc也有可能失败，怎么区分rpc失败和断连呢？
		// 一方面无限制地重试会增加开销，另外过期请求要如何处理？
		// 最大的问题是大量的过期请求在重试多次后会获取大量锁，应该怎么办呢？
		// return
		ok = rf.sendRequestVote(server, &args, &reply)
		if reply.IfOutedate {
			rf.rwmu.Lock()
			defer rf.rwmu.Unlock()
			if rf.term < reply.Term {
				rf.term = reply.Term
				ResetTimer(rf.heartbeatTimer, 300, 150)
				if rf.state != StateFollower {
					rf.state = StateFollower
					ResetTimer(rf.voteTimer, 1, 1)
					ResetTimer(rf.leaderTimer, 1, 1)
				}
				rf.persist()
			}
			return
		}
		rpccount++
		// fmt.Printf("me%v requestvote %v failed\n", rf.me, server)
		// 尝试给rpc次数增加限制。
		// 投票时间最长1000ms，超过这个次数投票时间也已经结束，可以退出
		if rpccount > 15 {
			return
		}
	}
	// 检查是不是无效票
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	if rf.state != StateCandidate || rf.ifstopvote || term != rf.term {
		return
	}

	if reply.Vote {
		rf.voteGets[term]++
	} else {
		rf.voteDisagree[term]++
	}
	// 即时检查选票
	if rf.voteGets[term] > rf.serverNum/2 || rf.voteDisagree[term] > rf.serverNum/2 {
		ResetTimer(rf.voteTimer, 1, 1)
		rf.ifstopvote = true
	}
}

func (rf *Raft) runLeader() {
	// fmt.Printf("me:%v is leader\n", rf.me)
	if rf.killed() {
		return
	}
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.startHeartBeat(i)
	}

	ResetTimer(rf.leaderTimer, 100, 1)
	<-rf.leaderTimer.C
}

func (rf *Raft) startHeartBeat(server int) {
	// 这里修改 因为存在锁释放又获取的情况，nextindex可能在这段时间改变
	// 所以为了避免错误修改，只获取函数执行时的index，并在后面检查是否需要修改
	rf.rwmu.RLock()
	term := rf.term
	commitindex := rf.commitIndex
	commitlogterm := rf.logs[commitindex-rf.snapoffset].Term
	curindex := rf.nextIndex - 1
	curlogterm := rf.logs[curindex-rf.snapoffset].Term
	rf.rwmu.RUnlock()

	args := HeartbeatsArgs{rf.me, term, commitindex, commitlogterm, curindex, curlogterm}
	// fmt.Printf("heartbeatsRPC%v\n", args)
	reply := HeartbeatsReply{}
	// leader向follower发送自己的commit与logindex，让follower知道自己是否落后
	ok := false
	rpccount := 0
	// fmt.Printf("heatbeatsreplyRPC%v\n", reply)
	for !ok {
		// fmt.Printf("server%v unconnect, return\n", server)
		ok = rf.peers[server].Call("Raft.HeartBeat", &args, &reply)
		if reply.IfOutedate {
			rf.rwmu.Lock()
			defer rf.rwmu.Unlock()
			if rf.term < reply.Term {
				rf.term = reply.Term
				ResetTimer(rf.heartbeatTimer, 300, 150)
				if rf.state != StateFollower {
					rf.state = StateFollower
					ResetTimer(rf.voteTimer, 1, 1)
					ResetTimer(rf.leaderTimer, 1, 1)
				}
				rf.persist()
			}
			return
		}
		rpccount++
		// fmt.Printf("me:%v heartbeat %v failed\n", rf.me, server)
		if rpccount > 3 { // 心跳的发送频率是100ms，超过时长后就有新的心跳发送
			return
		}
	}

	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()

	// check if still leader
	if term < rf.term {
		// 自己已经是旧的term了，不要执行任何行为
		return
	}
	if reply.CommitIndex > rf.commitIndex {
		// 出现oldcommit,不需要转为follower,而是直接commit自己的跟上进度
		// 作为强leader，不可能从follower处更新自己的log，所以直接commmit就是了
		// 添加commit次数限制 一次最多commit100条消息
		i := 0
		for rf.commitIndex < reply.CommitIndex {
			if rf.killed() {
				break
			}
			rf.commitIndex++
			i++
			fmt.Printf("me:%v commit log[%v]\n", rf.me, rf.commitIndex)
			msg := ApplyMsg{CommandValid: true, CommandIndex: rf.commitIndex, Command: rf.logs[rf.commitIndex-rf.snapoffset].Command}
			rf.applyCh <- msg
			if i == 100 {
				break
			}
		}
		rf.persist()
	}
	// still leader, check match

	if len(reply.SLogEntries) > 0 {
		if !rf.incheck[server] {
			go rf.MatchLog(server, reply.SLogEntries, reply.Start)
		}
	} else {
		// 不需要check，说明follower同步到了最新的curindex，把信息更新到leader中
		// 原本设计为matchindex[]不会下降的情况，尝试改为实时更新型
		// 还是修改成不会下降的情况，因为在leader的同一term内，matchindex是不会下降的，过时的忽略即可
		if rf.matchIndex[server] < reply.Start-1 {
			rf.matchIndex[server] = reply.Start - 1
			rf.commiter() // 有更新，检查一次commit
		}
	}
}

func (rf *Raft) HeartBeat(args *HeartbeatsArgs, reply *HeartbeatsReply) {
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	term := rf.term
	commitindex := rf.commitIndex
	// 全程持有锁的情况，把offset拷贝出来
	offset := rf.snapoffset
	// nextindex := rf.nextIndex

	reply.Me = rf.me
	reply.Term = term
	reply.CommitIndex = commitindex
	reply.SLogEntries = make([]SimpleLogEntry, 0)
	reply.Start = commitindex + 1
	// 理一理heartbeat的流程，首先检查这个是不是过期的。commit是不是落后的leader
	// 没有过期，commit也领先，可以准备commit
	// 在一致性的前提下进行，首先需要保证follower的长度到commit，因为前面节点的数据没有传过来。
	// 如果follower在commit处是对的，就可以进行commit。
	// 结束后检查follower到leader的nextindex是否一致，如果不一致，就要求同步。

	if args.Term < term {
		// fmt.Printf("%v is old term leader, ignore\n", args.Me)
		reply.IfOutedate = true
		return
	} else if args.Term > term {
		rf.term = args.Term
		rf.persist()
		rf.state = StateFollower
		ResetTimer(rf.heartbeatTimer, 300, 150)
		ResetTimer(rf.voteTimer, 1, 1)
		ResetTimer(rf.leaderTimer, 1, 1)
	}
	// // 改变选举策略后，不需要再检测commitindex了
	// if args.CommitIndex < commitindex {
	// 	fmt.Printf("%v is old commit leader, ignore\n", args.Me)
	// 	return
	// }
	rf.state = StateFollower
	ResetTimer(rf.heartbeatTimer, 300, 150)
	ResetTimer(rf.voteTimer, 1, 1)
	ResetTimer(rf.leaderTimer, 1, 1)

	// follower进行commit
	if args.CommitIndex < rf.nextIndex && args.CommitTerm == rf.logs[args.CommitIndex-offset].Term {
		// 添加commit次数限制 一次最多commit100条消息
		i := 0
		for rf.commitIndex < args.CommitIndex {
			if rf.killed() {
				break
			}
			rf.commitIndex++
			i++
			fmt.Printf("me:%v commit log[%v]\n", rf.me, rf.commitIndex)
			msg := ApplyMsg{CommandValid: true, CommandIndex: rf.commitIndex, Command: rf.logs[rf.commitIndex-offset].Command}
			rf.applyCh <- msg
			if i == 100 {
				break
			}
		}
		reply.CommitIndex = rf.commitIndex
		rf.persist()
	}

	// 确定需要matchindex的范围
	end := args.CurIndex
	start := rf.commitIndex + 1
	// 检查follower和leader在commitindex和curindex是否一致，确认发送一致性请求的范围
	// if args.CommitIndex >= nextindex || args.CommitTerm != rf.logs[args.CommitIndex].Term {
	// 	start = rf.commitIndex + 1
	// } else if args.CurIndex >= nextindex || args.CurTerm != rf.logs[args.CurIndex].Term {
	// 	start = args.CommitIndex + 1
	// } else {
	// 	start = args.CurIndex + 1
	// 	reply.Start = start
	// 	return
	// }
	// fmt.Printf("me:%v need match %v to %v\n", rf.me, start, end)

	// 这里遇到了奇怪的数组越界，原因是延迟rpc传来的curindex已经比followercommit的落后了，直接返回0即可
	if args.CurIndex < rf.commitIndex {
		fmt.Printf("me:%v old curindex%v < commitindex%v, ignore\n", rf.me, args.CurIndex, rf.commitIndex)
		return
	}
	// 这里为什么follower是空的也能正常进入matchindex呢？
	// 因为已经提前开辟了缺少的空间，len(log)的判定就不是0了
	reply.SLogEntries = make([]SimpleLogEntry, end-start+1)
	reply.Start = start
	// 如果follower所有log都commit，这里的长度就是0，需要leader额外判断
	for i := 0; i < end-start+1; i++ {
		if start+i >= len(rf.logs) {
			break
		}
		reply.SLogEntries[i].Index = rf.logs[start+i-offset].Index
		reply.SLogEntries[i].Term = rf.logs[start+i-offset].Term
	}
}
