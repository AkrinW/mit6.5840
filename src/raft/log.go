package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// 简化版entry，用来matchlog，
type SimpleLogEntry struct {
	Term  int
	Index int
}

type CheckMatchLogArgs struct {
	Me         int
	CurTerm    int      // 当前任期
	Log        LogEntry // 直接传一个logentry
	MatchIndex int      // Leader处已存的确认同步数
}

type CheckMatchLogReply struct {
	Me         int
	CurTerm    int
	IfContinue bool // Follower提示Leader是否要继续发前一个Log的信息
}

type SyncLogEntryArgs struct {
	Me      int
	CurTerm int
	Log     []LogEntry
}

type SyncLogEntryReply struct {
	Me      int
	CurTerm int
}

// type CommitLogArgs struct {
// 	Me      int
// 	CurTerm int
// 	Index   int
// }

// type CommitLogReply struct {
// 	Me      int
// 	CurTerm int
// }

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

// 注意注释里的要求，这个start函数要立刻返回，所以消息同步的内容用goroutine完成
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (3B).
	// fmt.Printf("me:%v Start %v\n", rf.me, command)
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	rf.termmu.RLock()
	newlog := LogEntry{command, rf.term, rf.nextIndex}
	rf.termmu.RUnlock()
	rf.matchIndex[rf.me] = rf.nextIndex
	rf.nextIndex++
	rf.logs = append(rf.logs, newlog)
	rf.persist()
	rf.mu.Unlock()
	fmt.Printf("me:%v append log%v, term%v, index%v\n", rf.me, command, newlog.Term, newlog.Index)
	// fmt.Printf("loginfo0:%v", rf.logs[0].Term)
	index := newlog.Index
	term := newlog.Term
	// msg := ApplyMsg{CommandValid: true, CommandIndex: index, Command: command}
	// rf.applyCh <- msg
	// 在每次start后启动有点问题。需要修改一下逻辑
	// test提供的等待时间是溢出的，只在选举稳定时进行log同步
	// 利用heartbeat进行同步

	// go rf.syncLog(index, term)
	return index, term, true
}

func (rf *Raft) commiter() {
	for !rf.killed() {
		time.Sleep(200 * time.Millisecond)
		// rf.mu.Lock()
		// state := rf.state
		// rf.mu.Unlock()
		// if state != StateLeader {
		// 	return
		// }
		// 先考虑不降级leader的情况
		// fmt.Printf("me:%v every0.2s check if commit\n", rf.me)
		// 这里问题出现在哪里呢？因为这里出现释放锁后又重复获取的情况，在之前判定通过后
		// 锁被别的线程取走占用了很长时间，而这段时间rf已经不再是leader了，却没有进行判定
		// 简单的放在一起即可，每次commit时全程占有锁
		rf.mu.Lock()
		if rf.killed() {
			// fmt.Printf("me:%v killed, stop commiter\n", rf.me)
			rf.mu.Unlock()
			return
		}
		fmt.Printf("me:%v every0.2s check if commit\n", rf.me)
		if rf.state != StateLeader {
			fmt.Printf("me:%v not leader return\n", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.matchIndex[rf.me] = rf.nextIndex - 1
		matchIndex := make([]int, rf.serverNum)
		copy(matchIndex, rf.matchIndex)
		sort.Ints(matchIndex)
		fmt.Printf("matchindex:%v\n", rf.matchIndex)
		if rf.commitIndex >= matchIndex[rf.serverNum/2] {
			fmt.Printf("no new commits\n")
			rf.mu.Unlock()
			continue
		}
		index := matchIndex[rf.serverNum/2]
		rf.termmu.RLock()
		if rf.logs[index].Term < rf.term {
			fmt.Printf("new match log%v, but old term%v not commit\n", rf.logs[index], rf.term)
			rf.mu.Unlock()
			rf.termmu.RUnlock()
			continue
		}
		rf.termmu.RUnlock()
		for rf.commitIndex < matchIndex[rf.serverNum/2] {
			rf.commitIndex++
			msg := ApplyMsg{CommandValid: true, CommandIndex: rf.commitIndex, Command: rf.logs[rf.commitIndex].Command}
			rf.applyCh <- msg
			fmt.Printf("me:%v once commit one new log %v\n", rf.me, rf.commitIndex)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) MatchLog(server int, slogentry []SimpleLogEntry, commitindex int) {
	// 先一个个往前遍历，寻找最后一个同步的节点
	// leader向follower从后往前发送index与term，找到第一个相同的。
	rf.mu.Lock()
	rf.incheck[server] = true
	// 再次重构，需要match的follower一次性发送大量节点，leader进行比较后确认从哪里开始同步
	i := 0
	index := commitindex
	for i < len(slogentry) {
		index = slogentry[i].Index
		if rf.logs[index].Term != slogentry[i].Term {
			index--
			break
		}
		i++
	}
	index++
	i = index
	logentries := make([]LogEntry, rf.nextIndex-index)
	for index < rf.nextIndex {
		logentries[index-i] = rf.logs[index]
		index++
	}
	rf.mu.Unlock()
	rf.termmu.RLock()
	args := SyncLogEntryArgs{rf.me, rf.term, logentries}
	reply := SyncLogEntryReply{}
	rf.termmu.RUnlock()
	fmt.Printf("me:%v sync server:%v's log%v to %v\n", rf.me, server, i, index-1)
	// fmt.Printf("logs:%v\n", args.Log)
	ok := false
	rpccount := 0
	for !ok {
		ok = rf.sendSyncLog(server, &args, &reply)
		time.Sleep(50 * time.Millisecond)
		rpccount++
		if rpccount > 3 {
			rf.mu.Lock()
			rf.incheck[server] = false
			rf.mu.Unlock()
			return
		}
		// ok = rf.sendMatchLog(server, &args, &reply)
		// time.Sleep(50 * time.Millisecond)
	}
	// 检查自己是否过时了
	rf.mu.Lock()
	rf.termmu.Lock()
	defer rf.mu.Unlock()
	defer rf.termmu.Unlock()
	if rf.term < reply.CurTerm {
		fmt.Printf("me:%v is old term, change to follower\n", rf.me)
		rf.term = reply.CurTerm
		rf.state = StateFollower
		ResetTimer(rf.heartbeatTimer, 200, 150)
		rf.incheck[server] = false
		rf.persist()
		return
	}
	rf.matchIndex[server] = index - 1
	rf.incheck[server] = false

	// matchIndex := rf.nextIndex - 1
	// curIndex := matchIndex
	// rf.mu.Unlock()
	// for matchIndex > 0 {
	// 	rf.mu.Lock()
	// 	args := CheckMatchLogArgs{rf.me, rf.term, rf.logs[matchIndex], rf.matchIndex[server]}
	// 	rf.mu.Unlock()
	// 	reply := CheckMatchLogReply{}
	// 	fmt.Printf("me:%v check server:%v's log[%v]\n", rf.me, server, matchIndex)
	// 	// 在checkmatch的过程中被disconnected了。只传输了部分数据。
	// 	// leader方面无法进行调整，应该把传一半设计为没传的情况。在follow进行修改
	// 	ok := rf.sendMatchLog(server, &args, &reply)
	// 	if !ok {
	// 		rf.mu.Lock()
	// 		rf.incheck[server] = false
	// 		rf.mu.Unlock()
	// 		return
	// 		// ok = rf.sendMatchLog(server, &args, &reply)
	// 		// time.Sleep(50 * time.Millisecond)
	// 	}

	// 	// 检查自己是否过时了
	// 	rf.mu.Lock()
	// 	if rf.term < reply.CurTerm {
	// 		fmt.Printf("me:%v is old term, change to follower\n", rf.me)
	// 		rf.term = reply.CurTerm
	// 		rf.state = StateFollower
	// 		ResetTimer(rf.heartbeatTimer, 200, 150)
	// 		rf.incheck[server] = false
	// 		rf.persist()
	// 		rf.mu.Unlock()
	// 		return
	// 	}
	// 	rf.mu.Unlock()

	// if !reply.IfContinue {
	// 	rf.mu.Lock()
	// 	// fmt.Printf("change matchindex[%v] in matchlog\n", server)
	// 	rf.matchIndex[server] = curIndex
	// 	rf.incheck[server] = false
	// 	rf.mu.Unlock()
	// 	return
	// 	} else {
	// 		// 检查前一个index是否匹配
	// 		matchIndex--
	// 	}
	// }

	// // 找到后，把后面的entrylog一个个传入，保证一致性
	// fmt.Printf("server%v's matchindex is %v\n", server, matchIndex)
	// for matchIndex < curIndex {
	// 	matchIndex++
	// 	rf.mu.Lock()
	// 	args := SyncLogEntryArgs{rf.me, rf.term, rf.logs[matchIndex]}
	// 	rf.mu.Unlock()
	// 	fmt.Printf("me:%v sync server%v's log[%v]\n", rf.me, server, matchIndex)
	// 	ok := false
	// 	reply := SyncLogEntryReply{}
	// 	// for !ok {
	// 	ok = rf.sendSyncLog(server, &args, &reply)
	// 	if !ok {
	// 		return
	// 	}
	// 	// time.Sleep(50 * time.Millisecond)
	// 	// }

	// 	// 反复检查自己是否过时了
	// 	rf.mu.Lock()
	// 	if rf.term < reply.CurTerm {
	// 		fmt.Printf("me:%v is old term, change to follower\n", rf.me)
	// 		rf.term = reply.CurTerm
	// 		rf.state = StateFollower
	// 		ResetTimer(rf.heartbeatTimer, 200, 150)
	// 		rf.mu.Unlock()
	// 		return
	// 	}
	// 	// 传完后，给leader发送信号，表示可以commit
	// 	rf.matchIndex[server] = matchIndex
	// 	rf.mu.Unlock()
	// }

	// leader commit完后 再要求各followercommit
}

// func (rf *Raft) sendMatchLog(server int, args *CheckMatchLogArgs, reply *CheckMatchLogReply) bool {
// 	ok := rf.peers[server].Call("Raft.CheckMatchLog", args, reply)
// 	return ok
// }

// func (rf *Raft) CheckMatchLog(args *CheckMatchLogArgs, reply *CheckMatchLogReply) {
// 	// Your code here (3A, 3B).
// 	reply.Me = rf.me
// 	reply.CurTerm = args.CurTerm
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if args.CurTerm < rf.term {
// 		// old request , refuse
// 		fmt.Printf("me:%v reci old leader check match,ignore\n", rf.me)
// 		reply.CurTerm = rf.term
// 		reply.IfContinue = false
// 		return
// 	}
// 	// 额外存储一个数据，为接受matchlog时，follower的term，
// 	// 如果传一半又有新的match请求，就把原有的tmplog清空再执行
// 	if args.CurTerm > rf.tmpterm {
// 		rf.tmpterm = args.CurTerm
// 		rf.tmplogs = []LogEntry{}
// 	}
// 	if rf.state != StateFollower || rf.term != args.CurTerm {
// 		rf.state = StateFollower
// 		rf.term = args.CurTerm
// 		rf.persist()
// 	}
// 	ResetTimer(rf.heartbeatTimer, 200, 150)
// 	rf.tmplogs = append(rf.tmplogs, args.Log)
// 	if args.Log.Index == args.MatchIndex+1 || args.Log.Index == rf.commitIndex+1 {
// 		reply.IfContinue = false
// 		// 到这里就已经全部同步了 ，倒序把tmplog插入到log里，再清空tmplog
// 		for tmp := len(rf.tmplogs); tmp > 0; {
// 			tmp--
// 			index := rf.tmplogs[tmp].Index
// 			// 总觉得这里直接用rf.nextindex有风险，还是直接获取log长度比较稳定
// 			if index < len(rf.logs) {
// 				rf.logs[index] = rf.tmplogs[tmp]
// 			} else {
// 				rf.logs = append(rf.logs, rf.tmplogs[tmp])
// 			}
// 			// rf.logs[rf.tmplogs[tmp].Index] = rf.tmplogs[tmp]
// 			// rf.logs = append(rf.logs, rf.tmplogs[tmp])
// 			fmt.Printf("me:%v sync log[%v] from%v\n", rf.me, index, args.Me)
// 		}
// 		rf.nextIndex = len(rf.logs)
// 		rf.tmplogs = []LogEntry{}
// 		rf.persist()
// 	} else {
// 		reply.IfContinue = true
// 	}
// 	// rf.persist()
// 	// if args.Index >= rf.nextIndex {
// 	// 	reply.IfMatch = false
// 	// 	fmt.Printf("me:%v dont get log[%v]\n", rf.me, args.Index)
// 	// } else if args.LogTerm != rf.logs[args.Index].Term {
// 	// 	reply.IfMatch = false
// 	// 	fmt.Printf("me:%v in log[%v] has diff term%v\n", rf.me, args.Index, rf.logs[args.Index].Term)
// 	// } else {
// 	// 	reply.IfMatch = true
// 	// 	fmt.Printf("me:%v in log[%v] is match\n", rf.me, args.Index)
// 	// }
// }

// // 对于每次start，启动一个goroutine去同步logs，当一半以上都同步时，commit
// func (rf *Raft) syncLog(index int, term int) {
// 	fmt.Printf("me:%v in term:%v start sync\n", rf.me, term)
// 	ResetTimer(rf.logTimer, 300, 300)
// 	for i := 0; i < rf.serverNum; i++ {
// 		if i == rf.me {
// 			continue
// 		}
// 		go rf.MatchLog(i)
// 	}

// 	<-rf.logTimer.C
// 	fmt.Printf("me:%v logtimeout,check if commit\n", rf.me)
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// 注意不能直接赋值，会排序会影响原数组
// 	matchIndex := make([]int, rf.serverNum)
// 	copy(matchIndex, rf.matchIndex)
// 	sort.Ints(matchIndex)
// 	// fmt.Printf("sort:%v\n", matchIndex)
// 	// fmt.Printf("rf.matchindex:%v\n", rf.matchIndex)
// 	if matchIndex[rf.serverNum/2] < index {
// 		// 未能让一半节点同步，认为自己已不是leader
// 		fmt.Printf("me:%v less half followers\n", rf.me)
// 		rf.state = StateFollower
// 		ResetTimer(rf.heartbeatTimer, 200, 150)
// 		return
// 	}
// 	// 获取一半同步，将自己commit
// 	fmt.Printf("me:%v commit log[%v]\n", rf.me, index)
// 	msg := ApplyMsg{CommandValid: true, CommandIndex: index, Command: rf.logs[index].Command}
// 	rf.applyCh <- msg
// 	rf.commitIndex = index
// 	// 通知其他可以commit的follower进行commit
// 	for i := 0; i < rf.serverNum; i++ {
// 		if i == rf.me || rf.matchIndex[i] < index {
// 			continue
// 		}
// 		go rf.CommitLog(i, index)
// 	}
// }

func (rf *Raft) sendSyncLog(server int, args *SyncLogEntryArgs, reply *SyncLogEntryReply) bool {
	ok := rf.peers[server].Call("Raft.SyncLog", args, reply)
	return ok
}

func (rf *Raft) SyncLog(args *SyncLogEntryArgs, reply *SyncLogEntryReply) {
	reply.Me = rf.me
	reply.CurTerm = args.CurTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.termmu.RLock()
	if args.CurTerm < rf.term {
		// old request , refuse
		fmt.Printf("me:%v reci old leader check match,ignore\n", rf.me)
		reply.CurTerm = rf.term
		rf.termmu.RUnlock()
		return
	}
	rf.termmu.RUnlock()
	rf.termmu.Lock()
	if rf.state != StateFollower || rf.term != args.CurTerm {
		rf.state = StateFollower
		rf.term = args.CurTerm
	}
	rf.termmu.Unlock()
	ResetTimer(rf.heartbeatTimer, 200, 150)
	index := 0
	// fmt.Printf("me:%v log before sync%v\n", rf.me, rf.logs)
	for i := 0; i < len(args.Log); i++ {
		index = args.Log[i].Index
		// 修改这里赋值log的逻辑，注意不能用nextindex修改，而要用log的长度修改，
		// 因为nextindex和log实际长度不对应会导致错误的append
		if index < len(rf.logs) {
			rf.logs[index] = args.Log[i]
		} else {
			rf.logs = append(rf.logs, args.Log[i])
		}
	}
	rf.matchIndex[rf.me] = index
	rf.nextIndex = index + 1
	rf.persist()
	// fmt.Printf("me:%v log after sync%v\n", rf.me, rf.logs)

	fmt.Printf("me:%v sync from%v log%v to %v\n", rf.me, args.Me, args.Log[0].Index, index)
}

// func (rf *Raft) SyncLog(args *SyncLogEntryArgs, reply *SyncLogEntryReply) {
// 	reply.Me = rf.me
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if args.CurTerm < rf.term {
// 		// old request , refuse
// 		fmt.Printf("me:%v reci old leader sync log,ignore\n", rf.me)
// 		reply.CurTerm = rf.term
// 		return
// 	}
// 	rf.state = StateFollower
// 	rf.term = args.CurTerm
// 	ResetTimer(rf.heartbeatTimer, 200, 150)
// 	for rf.nextIndex <= args.Log.Index {
// 		rf.logs = append(rf.logs, LogEntry{})
// 		rf.nextIndex++
// 	}
// 	rf.logs[args.Log.Index] = args.Log
// 	fmt.Printf("me:%v sync log[%v]\n", rf.me, args.Log.Index)
// }

// func (rf *Raft) CommitLog(server int, index int) {
// 	rf.mu.Lock()
// 	args := CommitLogArgs{rf.me, rf.term, index}
// 	reply := CommitLogReply{}
// 	rf.mu.Unlock()
// 	ok := rf.sendCommitLog(server, &args, &reply)
// 	if !ok {
// 		return
// 		// ok = rf.sendCommitLog(server, &args, &reply)
// 		// time.Sleep(50 * time.Millisecond)
// 	}

// 	// 检查自己是否过时了
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if rf.term < reply.CurTerm {
// 		fmt.Printf("me:%v is old term, change to follower\n", rf.me)
// 		rf.term = reply.CurTerm
// 		rf.state = StateFollower
// 		ResetTimer(rf.heartbeatTimer, 200, 150)
// 	}
// }

// func (rf *Raft) sendCommitLog(server int, args *CommitLogArgs, reply *CommitLogReply) bool {
// 	ok := rf.peers[server].Call("Raft.FollowerCommitLog", args, reply)
// 	return ok
// }

// func (rf *Raft) FollowerCommitLog(args *CommitLogArgs, reply *CommitLogReply) {
// 	reply.Me = rf.me
// 	reply.CurTerm = args.CurTerm
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if args.CurTerm < rf.term {
// 		// old request , refuse
// 		fmt.Printf("me:%v reci old leader commit log,ignore\n", rf.me)
// 		reply.CurTerm = rf.term
// 		return
// 	}
// 	ResetTimer(rf.heartbeatTimer, 200, 150)
// 	if args.Index < rf.commitIndex {
// 		fmt.Printf("me:%v already commited %v\n", rf.me, args.Index)
// 		return
// 	}
// 	rf.state = StateFollower
// 	rf.term = args.CurTerm

// 	fmt.Printf("me:%v commit log[%v]\n", rf.me, args.Index)
// 	msg := ApplyMsg{CommandValid: true, CommandIndex: args.Index, Command: rf.logs[args.Index].Command}
// 	rf.applyCh <- msg
// 	rf.commitIndex = args.Index
// }

// func (rf *Raft) HeartCommitLog(server int, start int, target int) {
// 	// 对server进行commit，原理是对每个index进行同步和commit
// 	// 因为发送的都是已经commit的log，所以不需要担心term的问题，直接传log同步即可
// 	for start < target {
// 		start++
// 		rf.mu.Lock()
// 		if rf.state != StateLeader {
// 			fmt.Printf("me:%v is not leader, stop commit\n", rf.me)
// 			rf.mu.Unlock()
// 			return
// 		}
// 		args := CheckMatchLogArgs{rf.me, rf.term, rf.logs[start].Term, start}
// 		rf.mu.Unlock()
// 		reply := CheckMatchLogReply{}
// 		ok := rf.sendMatchLog(server, &args, &reply)
// 		fmt.Printf("me:%v check server:%v's log[%v]\n", rf.me, server, args.Index)
// 		if !ok {
// 			return
// 		}
// 		if !reply.IfMatch {
// 			rf.mu.Lock()
// 			args := SyncLogEntryArgs{rf.me, rf.term, rf.logs[start]}
// 			rf.mu.Unlock()
// 			fmt.Printf("me:%v heart sync server%v's log[%v]\n", rf.me, server, start)
// 			reply := SyncLogEntryReply{}
// 			ok := rf.sendSyncLog(server, &args, &reply)
// 			if !ok {
// 				return
// 				// ok = rf.sendSyncLog(server, &args, &reply)
// 				// time.Sleep(50 * time.Millisecond)
// 			}
// 			// 传完后，给leader发送信号，表示可以commit
// 			rf.mu.Lock()
// 			rf.matchIndex[server] = start
// 			rf.mu.Unlock()
// 		}
// 		rf.CommitLog(server, start)
// 	}
// }
