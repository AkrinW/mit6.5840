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

type CheckMatchLogArgs struct {
	Me      int
	CurTerm int
	LogTerm int
	Index   int
}

type CheckMatchLogReply struct {
	Me      int
	CurTerm int
	IfMatch bool
}

type SyncLogEntryArgs struct {
	Me      int
	CurTerm int
	Log     LogEntry
}

type SyncLogEntryReply struct {
	Me      int
	CurTerm int
}

type CommitLogArgs struct {
	Me      int
	CurTerm int
	Index   int
}

type CommitLogReply struct {
	Me      int
	CurTerm int
}

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
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	newlog := LogEntry{command, rf.term, rf.nextIndex}
	rf.matchIndex[rf.me] = rf.nextIndex
	rf.nextIndex++
	rf.logs = append(rf.logs, newlog)
	rf.mu.Unlock()
	fmt.Printf("me:%v append log, term%v, index%v\n", rf.me, newlog.Term, newlog.Index)
	// fmt.Printf("loginfo0:%v", rf.logs[0].Term)
	index := newlog.Index
	term := newlog.Term
	// msg := ApplyMsg{CommandValid: true, CommandIndex: index, Command: command}
	// rf.applyCh <- msg
	go rf.syncLog(index, term)
	return index, term, true
}

// 对于每次start，启动一个goroutine去同步logs，当一半以上都同步时，commit
func (rf *Raft) syncLog(index int, term int) {
	fmt.Printf("me:%v in term:%v start sync\n", rf.me, term)
	ResetTimer(rf.logTimer, 500, 500)
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.MatchLog(i)
	}

	<-rf.logTimer.C
	fmt.Printf("me:%v logtimeout,check if commit\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	matchIndex := rf.matchIndex
	sort.Ints(matchIndex)
	if matchIndex[rf.serverNum/2] < index {
		// 未能让一半节点同步，认为自己已不是leader
		fmt.Printf("me:%v less half followers\n", rf.me)
		rf.state = StateFollower
		ResetTimer(rf.heartbeatTimer, 200, 150)
		return
	}
	// 获取一半同步，将自己commit
	fmt.Printf("me:%v commit log[%v]\n", rf.me, index)
	msg := ApplyMsg{CommandValid: true, CommandIndex: index, Command: rf.logs[index].Command}
	rf.applyCh <- msg
	// 通知其他可以commit的follower进行commit
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me || rf.matchIndex[i] < index {
			continue
		}
		go rf.CommitLog(i, index)
	}
}

func (rf *Raft) MatchLog(server int) {
	// 先一个个往前遍历，寻找最后一个同步的节点
	// leader向follower从后往前发送index与term，找到第一个相同的。
	rf.mu.Lock()
	matchIndex := rf.nextIndex - 1
	curIndex := matchIndex
	rf.mu.Unlock()
	for matchIndex > -1 {
		rf.mu.Lock()
		args := CheckMatchLogArgs{rf.me, rf.term, rf.logs[matchIndex].Term, matchIndex}
		rf.mu.Unlock()
		reply := CheckMatchLogReply{}
		ok := false
		fmt.Printf("me:%v check server:%v's log[%v]\n", rf.me, server, args.Index)
		for !ok {
			ok = rf.sendMatchLog(server, &args, &reply)
			time.Sleep(50 * time.Millisecond)
		}

		// 检查自己是否过时了
		rf.mu.Lock()
		if rf.term < reply.CurTerm {
			fmt.Printf("me:%v is old term, change to follower\n", rf.me)
			rf.term = reply.CurTerm
			rf.state = StateFollower
			ResetTimer(rf.heartbeatTimer, 200, 150)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if reply.IfMatch {
			break
		} else {
			// 检查前一个index是否匹配
			matchIndex--
		}
	}
	// 找到后，把后面的entrylog一个个传入，保证一致性
	fmt.Printf("server%v's matchindex is %v\n", server, matchIndex)
	for matchIndex < curIndex {
		matchIndex++
		rf.mu.Lock()
		args := SyncLogEntryArgs{rf.me, rf.term, rf.logs[matchIndex]}
		rf.mu.Unlock()
		fmt.Printf("me:%v sync server%v's log[%v]\n", rf.me, server, matchIndex)
		ok := false
		reply := SyncLogEntryReply{}
		for !ok {
			ok = rf.sendSyncLog(server, &args, &reply)
			time.Sleep(50 * time.Millisecond)
		}

		// 反复检查自己是否过时了
		rf.mu.Lock()
		if rf.term < reply.CurTerm {
			fmt.Printf("me:%v is old term, change to follower\n", rf.me)
			rf.term = reply.CurTerm
			rf.state = StateFollower
			ResetTimer(rf.heartbeatTimer, 200, 150)
			rf.mu.Unlock()
			return
		}
		// 传完后，给leader发送信号，表示可以commit
		rf.matchIndex[server] = matchIndex
		rf.mu.Unlock()
	}

	// leader commit完后 再要求各followercommit
}

func (rf *Raft) sendMatchLog(server int, args *CheckMatchLogArgs, reply *CheckMatchLogReply) bool {
	ok := rf.peers[server].Call("Raft.CheckMatchLog", args, reply)
	return ok
}

func (rf *Raft) CheckMatchLog(args *CheckMatchLogArgs, reply *CheckMatchLogReply) {
	// Your code here (3A, 3B).
	reply.Me = rf.me
	reply.CurTerm = args.CurTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CurTerm < rf.term {
		// old request , refuse
		fmt.Printf("me:%v reci old leader check match,ignore\n", rf.me)
		reply.CurTerm = rf.term
		reply.IfMatch = false
		return
	}
	rf.state = StateFollower
	rf.term = args.CurTerm
	ResetTimer(rf.heartbeatTimer, 200, 150)
	if args.Index >= rf.nextIndex {
		reply.IfMatch = false
		fmt.Printf("me:%v dont get log[%v]\n", rf.me, args.Index)
	} else if args.LogTerm != rf.logs[args.Index].Term {
		reply.IfMatch = false
		fmt.Printf("me:%v in log[%v] has diff term%v\n", rf.me, args.Index, rf.logs[args.Index].Term)
	} else {
		reply.IfMatch = true
		fmt.Printf("me:%v in log[%v] is match\n", rf.me, args.Index)
	}
}

func (rf *Raft) sendSyncLog(server int, args *SyncLogEntryArgs, reply *SyncLogEntryReply) bool {
	ok := rf.peers[server].Call("Raft.SyncLog", args, reply)
	return ok
}

func (rf *Raft) SyncLog(args *SyncLogEntryArgs, reply *SyncLogEntryReply) {
	reply.Me = rf.me
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CurTerm < rf.term {
		// old request , refuse
		fmt.Printf("me:%v reci old leader sync log,ignore\n", rf.me)
		reply.CurTerm = rf.term
		return
	}
	rf.state = StateFollower
	rf.term = args.CurTerm
	ResetTimer(rf.heartbeatTimer, 200, 150)
	for rf.nextIndex <= args.Log.Index {
		rf.logs = append(rf.logs, LogEntry{})
		rf.nextIndex++
	}
	rf.logs[args.Log.Index] = args.Log
	fmt.Printf("me:%v sync log[%v]\n", rf.me, args.Log.Index)
}

func (rf *Raft) CommitLog(server int, index int) {
	rf.mu.Lock()
	args := CommitLogArgs{rf.me, rf.term, index}
	reply := CommitLogReply{}
	rf.mu.Unlock()
	ok := false
	for !ok {
		ok = rf.sendCommitLog(server, &args, &reply)
		time.Sleep(50 * time.Millisecond)
	}

	// 检查自己是否过时了
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term < reply.CurTerm {
		fmt.Printf("me:%v is old term, change to follower\n", rf.me)
		rf.term = reply.CurTerm
		rf.state = StateFollower
		ResetTimer(rf.heartbeatTimer, 200, 150)
	}
}

func (rf *Raft) sendCommitLog(server int, args *CommitLogArgs, reply *CommitLogReply) bool {
	ok := rf.peers[server].Call("Raft.FollowerCommitLog", args, reply)
	return ok
}

func (rf *Raft) FollowerCommitLog(args *CommitLogArgs, reply *CommitLogReply) {
	reply.Me = rf.me
	reply.CurTerm = args.CurTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CurTerm < rf.term {
		// old request , refuse
		fmt.Printf("me:%v reci old leader commit log,ignore\n", rf.me)
		reply.CurTerm = rf.term
		return
	}
	rf.state = StateFollower
	rf.term = args.CurTerm
	ResetTimer(rf.heartbeatTimer, 200, 150)

	fmt.Printf("me:%v commit log[%v]\n", rf.me, args.Index)
	msg := ApplyMsg{CommandValid: true, CommandIndex: args.Index, Command: rf.logs[args.Index].Command}
	rf.applyCh <- msg
}
