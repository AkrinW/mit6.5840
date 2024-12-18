package raft

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
	CommitTerm  int
	// IfNeedMatch bool             // 返回自己是否需要同步
	SLogEntries []SimpleLogEntry //返回从commit开始的所有log，用于比较一致性
	Start       int              // 用来提示slog的开头是哪个index
	IfOutedate  bool
}

// 重写思路，用读写锁的形式，在函数开头用读锁获取全部数据
// 需要修改的部分放在结尾，用写锁一并更新

// 需要注意数据争用的问题，比较关键的数据类型每次读或写都需要加锁，保证原子性
func (rf *Raft) ticker() {
	ResetTimer(rf.heartbeatTimer, 250, 100)
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		// DPrintf("me:%v state%v\n", rf.me, rf.state)

		rf.rwmu.RLock()
		state := rf.state
		// DPrintf("me:%v term:%v state:%v commit:%v next:%v loglen:%v match:%v\n", rf.me, rf.term, rf.state, rf.commitIndex, rf.nextIndex, len(rf.logs), rf.matchIndex)
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
	// DPrintf("me:%v is follower\n", rf.me)

	<-rf.heartbeatTimer.C
	if rf.killed() {
		return
	}
	// DPrintf("me:%v heartbeater timeout\n", rf.me)

	rf.rwmu.Lock()
	rf.state = StateCandidate
	// 开始投票，初始化自己的选票状态
	rf.term++
	term := rf.term
	// DPrintf("me:%v term%v\n", rf.me, rf.term)
	rf.voteTo[rf.term] = rf.me
	rf.voteGets[rf.term] = make(map[int]bool, rf.serverNum)
	rf.voteGets[rf.term][rf.me] = true
	rf.voteDisagree[rf.term] = make(map[int]bool, rf.serverNum)
	rf.ifstopvote = false
	rf.persist()
	ResetTimer(rf.voteTimer, 200, 400) // 时间重置需要和锁一起

	rf.rwmu.Unlock()

	// start vote
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.startVote(i, term)
	}
}

func (rf *Raft) TurntoFollower() {
	rf.state = StateFollower
	ResetTimer(rf.heartbeatTimer, 250, 100)
	ResetTimer(rf.voteTimer, 1, 1)
	ResetTimer(rf.leaderTimer, 1, 1)
}

func (rf *Raft) runCandidate() {
	// DPrintf("me:%v is candidate\n", rf.me)

	// 将candidate的选票逻辑移动到follower了，目的是为了保持term变化的原子性

	<-rf.voteTimer.C
	if rf.killed() {
		return
	}
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	// DPrintf("me:%v vote time out, check if leader\n", rf.me)
	rf.ifstopvote = true
	if rf.state == StateFollower {
		// DPrintf("me:%v become follower\n", rf.me)
		rf.TurntoFollower()
		return
	}
	sum := 0
	for i := 0; i < rf.serverNum; i++ {
		if rf.voteGets[rf.term][i] {
			sum++
		}
	}
	if sum > rf.serverNum/2 {
		// DPrintf("me:%v term%v become leader\n", rf.me, rf.term)
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
		// DPrintf("me:%v term%v vote failed\n", rf.me, rf.term)
		rf.TurntoFollower()
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
	// commitindex := rf.commitIndex
	curindex := rf.nextIndex - 1
	offset := rf.snapoffset
	// 所有rf.logs[]的操作都减去一个offset
	curterm := rf.logs[curindex-offset].Term
	voteto, exists := rf.voteTo[args.Term]

	reply.Term = term
	reply.Vote = false
	if args.Term < term {
		reply.IfOutedate = true
		// DPrintf("me:%v refuse vote %v, term%v, old term\n", rf.me, args.Me, reply.Term)
		return
	} else if args.Term == term {
		// 同一任期
		if !exists {
			if args.CurTerm > curterm {
				reply.Vote = true
			}
			if args.CurTerm == curterm {
				if args.CurIndex >= curindex {
					reply.Vote = true
				}
				// if args.CurIndex == curindex && args.CommitIndex >= commitindex {
				// 	reply.Vote = true
				// }
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
		} else if voteto == args.Me {
			reply.Vote = true
		}
	} else {
		if args.CurTerm > curterm {
			reply.Vote = true
		}
		if args.CurTerm == curterm {
			if args.CurIndex >= curindex {
				reply.Vote = true
			}
			// if args.CurIndex == curindex && args.CommitIndex >= commitindex {
			// 	reply.Vote = true
			// }
		}
		rf.term = args.Term
		rf.TurntoFollower()

		rf.persist()
	}
	reply.Term = rf.term
	if reply.Vote {
		rf.TurntoFollower()
		rf.voteTo[args.Term] = args.Me
		// DPrintf("me:%v vote %v, term%v\n", rf.me, args.Me, reply.Term)
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

func (rf *Raft) startVote(server int, startterm int) {
	rf.rwmu.RLock()
	term := rf.term
	args := RequestVoteArgs{rf.me, term, rf.commitIndex, rf.nextIndex - 1, rf.logs[rf.nextIndex-1-rf.snapoffset].Term}
	reply := RequestVoteReply{}
	rf.rwmu.RUnlock()
	ok := false
	rpccount := 0
	if startterm != term {
		return
	}
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
				rf.TurntoFollower()
				rf.persist()
			}
			return
		}
		rpccount++
		// DPrintf("me%v requestvote %v failed\n", rf.me, server)
		// 尝试给rpc次数增加限制。
		// 投票时间最长1000ms，超过这个次数投票时间也已经结束，可以退出
		if rpccount > 6 {
			return
		}
	}
	// 检查是不是无效票
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	// DPrintf("me:%v revi term%v's vote%v from %v,curterm%v\n", rf.me, term, reply.Vote, reply.Me, rf.term)
	if rf.state != StateCandidate || rf.ifstopvote || term != rf.term {
		return
	}

	if reply.Vote && !rf.voteGets[term][reply.Me] {
		rf.voteGets[term][reply.Me] = true
		sumagree := 0
		for i := 0; i < rf.serverNum; i++ {
			if rf.voteGets[rf.term][i] {
				sumagree++
			}
		}
		if sumagree > rf.serverNum/2 {
			ResetTimer(rf.voteTimer, 1, 1)
			rf.ifstopvote = true
		}
	} else if !reply.Vote && !rf.voteDisagree[term][reply.Me] {
		rf.voteDisagree[term][reply.Me] = true
		sumdisagree := 0
		for i := 0; i < rf.serverNum; i++ {
			if rf.voteGets[rf.term][i] {
				sumdisagree++
			}
		}
		if sumdisagree > rf.serverNum/2 {
			ResetTimer(rf.voteTimer, 1, 1)
			rf.ifstopvote = true
		}
	}
	// 即时检查选票
}

func (rf *Raft) runLeader() {
	// DPrintf("me:%v is leader\n", rf.me)
	if rf.killed() {
		return
	}
	rf.rwmu.Lock()
	startterm := rf.term
	rf.rwmu.Unlock()
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.startHeartBeat(i, startterm)
	}

	ResetTimer(rf.leaderTimer, 100, 1)
	<-rf.leaderTimer.C
}

func (rf *Raft) startHeartBeat(server int, startterm int) {
	// 这里修改 因为存在锁释放又获取的情况，nextindex可能在这段时间改变
	// 所以为了避免错误修改，只获取函数执行时的index，并在后面检查是否需要修改
	rf.rwmu.RLock()
	if rf.state != StateLeader || rf.term != startterm {
		rf.rwmu.RUnlock()
		return
	}
	term := rf.term
	commitindex := rf.commitIndex
	commitlogterm := rf.logs[commitindex-rf.snapoffset].Term
	curindex := rf.nextIndex - 1
	curlogterm := rf.logs[curindex-rf.snapoffset].Term
	rf.rwmu.RUnlock()

	args := HeartbeatsArgs{rf.me, term, commitindex, commitlogterm, curindex, curlogterm}
	// DPrintf("heartbeatsRPC%v\n", args)
	reply := HeartbeatsReply{}
	// leader向follower发送自己的commit与logindex，让follower知道自己是否落后
	ok := false
	rpccount := 0
	// DPrintf("heatbeatsreplyRPC%v\n", reply)
	for !ok {
		// DPrintf("me:%v send heartbeat to server%v \n", rf.me, server)
		ok = rf.peers[server].Call("Raft.HeartBeat", &args, &reply)
		if reply.IfOutedate {
			rf.rwmu.Lock()
			defer rf.rwmu.Unlock()
			if rf.term < reply.Term {
				rf.term = reply.Term
				rf.TurntoFollower()
				rf.persist()
			}
			return
		}
		rpccount++
		// DPrintf("me:%v heartbeat %v failed\n", rf.me, server)
		if rpccount > 3 { // 心跳的发送频率是100ms，超过时长后就有新的心跳发送
			return
		}
	}

	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()

	// check if still leader
	if term < rf.term || reply.Term != rf.term || rf.state != StateLeader {
		// 自己已经是旧的term了，不要执行任何行为
		return
	}
	if reply.CommitIndex > rf.commitIndex && reply.Term == rf.term {
		// 保底检查一遍，对方的committerm是否与自己一致，存在落后leader收到对方已commit的情况
		// 这时候leader强制转为follower
		tmpterm := rf.logs[reply.CommitIndex-rf.snapoffset].Term
		if reply.CommitTerm != tmpterm {
			// DPrintf("me:%v leader wrong commit[%v] term%v from reply%v, become follower\n", rf.me, reply.CommitIndex, tmpterm, reply.CommitTerm)
			rf.TurntoFollower()
			return
		}
		// 出现oldcommit,不需要转为follower,而是直接commit自己的跟上进度
		// 作为强leader，不可能从follower处更新自己的log，所以直接commmit就是了
		// 添加commit次数限制 一次最多commit100条消息
		rf.committochan(reply.CommitIndex, "startHeartBeat")
	}
	// still leader, check match
	// 不需要check，说明follower同步到了最新的curindex，把信息更新到leader中
	// 原本设计为matchindex[]不会下降的情况，尝试改为实时更新型
	// 还是修改成不会下降的情况，因为在leader的同一term内，matchindex是不会下降的，过时的忽略即可
	if rf.matchIndex[server] < reply.Start-1 {
		rf.matchIndex[server] = reply.Start - 1
		rf.commiter() // 有更新，检查一次commit
	}
	// 在heartbeat时检查follower的commitindex，如果它落后到无法用matchlog同步
	// 就先调用installsnap同步
	if reply.Start <= rf.snapoffset {
		if !rf.ininstallsnap[server] {
			go rf.InstallSnapshot(server, rf.term)
		}
		return
	}
	if len(reply.SLogEntries) > 0 && rf.matchIndex[server] < rf.nextIndex-1 {
		if !rf.incheck[server] {
			go rf.MatchLog(server, reply.SLogEntries, reply.Start, rf.term)
		}
	} else {
		rf.commiter()
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
	reply.CommitTerm = rf.logs[commitindex-offset].Term
	reply.SLogEntries = make([]SimpleLogEntry, 0)
	reply.Start = commitindex + 1
	// 理一理heartbeat的流程，首先检查这个是不是过期的。commit是不是落后的leader
	// 没有过期，commit也领先，可以准备commit
	// 在一致性的前提下进行，首先需要保证follower的长度到commit，因为前面节点的数据没有传过来。
	// 如果follower在commit处是对的，就可以进行commit。
	// 结束后检查follower到leader的nextindex是否一致，如果不一致，就要求同步。

	if args.Term < term {
		// DPrintf("%v is old term leader, ignore\n", args.Me)
		reply.IfOutedate = true
		return
	} else if args.Term > term {
		rf.term = args.Term
		rf.persist()
	}
	reply.Term = rf.term
	// // 改变选举策略后，不需要再检测commitindex了
	// if args.CommitIndex < commitindex {
	// 	DPrintf("%v is old commit leader, ignore\n", args.Me)
	// 	return
	// }
	rf.TurntoFollower()

	// follower进行commit, 需要检查自己是不是落后的
	if args.CommitIndex < rf.nextIndex && args.CommitIndex > offset && args.CommitTerm == rf.logs[args.CommitIndex-offset].Term {
		// 添加commit次数限制 一次最多commit100条消息
		rf.committochan(args.CommitIndex, "HeartBeat")
		reply.CommitIndex = rf.commitIndex
		reply.CommitTerm = rf.logs[commitindex-offset].Term
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
	// DPrintf("me:%v need match %v to %v\n", rf.me, start, end)

	// 这里遇到了奇怪的数组越界，原因是延迟rpc传来的curindex已经比followercommit的落后了，直接返回0即可
	if args.CurIndex < rf.commitIndex {
		// DPrintf("me:%v old curindex%v < commitindex%v, ignore\n", rf.me, args.CurIndex, rf.commitIndex)
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
	// DPrintf("me:%v args:%v reply:%v %v\n", rf.me, args, reply.CommitIndex, reply.Start)

}
