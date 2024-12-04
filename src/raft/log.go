package raft

import (
	"fmt"
	"sort"
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

type SyncLogEntryArgs struct {
	Me          int
	CurTerm     int
	CommitIndex int
	Log         []LogEntry
}

type SyncLogEntryReply struct {
	Me          int
	CurTerm     int
	Flag        bool // 返回是否同步成功
	IfOutedate  bool
	CommitIndex int
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
	// fmt.Printf("me:%v Start %v\n", rf.me, command)
	// 针对并发写入的问题，给start添加全局写锁
	if rf.killed() {
		return -1, -1, false
	}
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	state := rf.state
	term := rf.term
	index := rf.nextIndex
	offset := rf.snapoffset

	if state != StateLeader {
		return -1, -1, false
	}
	newlog := LogEntry{command, term, index}

	// 注意扩充logs时，怎么判断logs的长度和index
	rf.matchIndex[rf.me] = index
	if len(rf.logs) > index-offset {
		rf.logs[index-offset] = newlog
	} else {
		rf.logs = append(rf.logs, newlog)
	}
	rf.nextIndex = index + 1
	rf.persist()
	fmt.Printf("me:%v append log%v, term%v, index%v\n", rf.me, command, term, index)

	return index, term, true

	// msg := ApplyMsg{CommandValid: true, CommandIndex: index, Command: command}
	// rf.applyCh <- msg
	// 在每次start后启动有点问题。需要修改一下逻辑
	// test提供的等待时间是溢出的，只在选举稳定时进行log同步
	// 利用heartbeat进行同步

	// go rf.syncLog(index, term)
}

// 为何commiter需要单独一个goroutine？
// follower在heartbeat时就进行commit，leader同样只需要在这种时候执行
// 没有锁的释放或获取，可以更安全
// 本质是checkmatch对可提交的进行commit，在每次更新commit时执行一次就行了。

func (rf *Raft) commiter() {
	if rf.killed() {
		return
	}
	// 先考虑不降级leader的情况
	// fmt.Printf("me:%v every0.2s check if commit\n", rf.me)
	// 这里问题出现在哪里呢？因为这里出现释放锁后又重复获取的情况，在之前判定通过后
	// 锁被别的线程取走占用了很长时间，而这段时间rf已经不再是leader了，却没有进行判定
	// 简单的放在一起即可，每次commit时全程占有锁
	fmt.Printf("me:%v check if commit\n", rf.me)
	if rf.state != StateLeader {
		fmt.Printf("me:%v not leader return\n", rf.me)
		return
	}
	matchIndex := make([]int, rf.serverNum)
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	offset := rf.snapoffset
	fmt.Printf("matchindex:%v\n", rf.matchIndex)
	if rf.commitIndex >= matchIndex[rf.serverNum/2] {
		fmt.Printf("no new commits\n")
		return
	}
	index := matchIndex[rf.serverNum/2]
	if rf.logs[index-offset].Term < rf.term {
		fmt.Printf("new match log%v but old, term%v not commit\n", rf.logs[index-offset], rf.term)
		return
	}
	i := 0
	for rf.commitIndex < matchIndex[rf.serverNum/2] {
		rf.commitIndex++
		msg := ApplyMsg{CommandValid: true, CommandIndex: rf.commitIndex, Command: rf.logs[rf.commitIndex-offset].Command}
		rf.applyCh <- msg
		fmt.Printf("me:%v once commit one new log %v\n", rf.me, rf.commitIndex)
		i++
		if i == 100 {
			break
		}
	}
	rf.persist()
}

func (rf *Raft) MatchLog(server int, slogentry []SimpleLogEntry, startindex int) {
	// 先一个个往前遍历，寻找最后一个同步的节点
	// leader向follower从后往前发送index与term，找到第一个相同的。
	rf.rwmu.Lock()
	term := rf.term
	index := startindex
	offset := rf.snapoffset
	if index < offset {
		// 因为snapshot的原因，已经构建不出checklog了，logs[0]是已经commit的节点，还可以使用
		// 如果要check负数，只能发送快照。
		// 在test4D1里应该不需要考虑，因为follower一直连接状态，leader总是同步完成才commit的
		fmt.Printf("me:%v log[%v] already snapshot, cant matchlog\n", rf.me, index)
		rf.rwmu.Unlock()
		return
	}
	i := 0
	for i = 0; i < len(slogentry); i++ {
		if slogentry[i].Term != rf.logs[index+i-offset].Term {
			break
		}
	}
	index = index + i
	i = index
	logentries := make([]LogEntry, rf.nextIndex-index)
	for index < rf.nextIndex {
		logentries[index-i] = rf.logs[index-offset]
		index++
	}
	// 这里可能更新，也可能不会更新
	if rf.matchIndex[server] < i-1 {
		rf.matchIndex[server] = i - 1
		rf.commiter()
	}
	if i == index {
		rf.rwmu.Unlock()
		return
	}
	rf.incheck[server] = true
	args := SyncLogEntryArgs{rf.me, term, rf.commitIndex, logentries}
	reply := SyncLogEntryReply{}
	rf.rwmu.Unlock()
	// fmt.Printf("slog:%v,comit%v,args%v\n", slogentry, commitindex, args)
	fmt.Printf("me:%v sync server:%v's log%v to %v\n", rf.me, server, i, index-1)
	// fmt.Printf("logs:%v\n", args.Log)
	ok := false
	rpccount := 0
	for !ok {
		ok = rf.sendSyncLog(server, &args, &reply)
		if reply.IfOutedate {
			rf.rwmu.Lock()
			defer rf.rwmu.Unlock()
			if rf.term < reply.CurTerm {
				rf.term = reply.CurTerm
				rf.incheck[server] = false
				ResetTimer(rf.heartbeatTimer, 500, 150)
				if rf.state != StateFollower {
					rf.state = StateFollower
					ResetTimer(rf.voteTimer, 5, 1)
					ResetTimer(rf.leaderTimer, 5, 1)
				}
				rf.persist()
			}
			return
		}
		rpccount++
		// fmt.Printf("me%v synclog %v failed\n", rf.me, server)
		if rpccount > 3 {
			rf.rwmu.Lock()
			rf.incheck[server] = false
			rf.rwmu.Unlock()
			return
		}
		// ok = rf.sendMatchLog(server, &args, &reply)
		// time.Sleep(50 * time.Millisecond)
	}

	// 检查自己是否过时了
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	rf.incheck[server] = false
	if rf.term < reply.CurTerm {
		fmt.Printf("me:%v is old term, change to follower\n", rf.me)
		rf.term = reply.CurTerm
		if rf.state != StateFollower {
			rf.state = StateFollower
			ResetTimer(rf.heartbeatTimer, 500, 150)
			ResetTimer(rf.leaderTimer, 5, 1)
			ResetTimer(rf.voteTimer, 5, 1)
		}
		rf.persist()
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
	if reply.Flag && rf.matchIndex[server] < index-1 {
		rf.matchIndex[server] = index - 1
		fmt.Printf("me:%v in matchlog, change matchindex[%v]=%v\n", rf.me, server, index-1)
		rf.commiter()
	}
}

func (rf *Raft) sendSyncLog(server int, args *SyncLogEntryArgs, reply *SyncLogEntryReply) bool {
	ok := rf.peers[server].Call("Raft.SyncLog", args, reply)
	return ok
}

func (rf *Raft) SyncLog(args *SyncLogEntryArgs, reply *SyncLogEntryReply) {
	reply.Me = rf.me
	reply.CurTerm = args.CurTerm
	reply.Flag = false
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	if args.CurTerm < rf.term || args.CommitIndex < rf.commitIndex {
		// old request , refuse
		fmt.Printf("me:%v reci old leader check match,ignore\n", rf.me)
		reply.CurTerm = rf.term
		return
	}
	reply.CommitIndex = rf.commitIndex
	if args.CurTerm > rf.term {
		rf.term = args.CurTerm
		rf.persist()
	}
	if rf.state != StateFollower {
		rf.state = StateFollower
		ResetTimer(rf.voteTimer, 5, 1)
		ResetTimer(rf.leaderTimer, 5, 1)
	}
	ResetTimer(rf.heartbeatTimer, 500, 150)
	if len(args.Log) == 0 {
		fmt.Printf("me:%v sync nothing\n", rf.me)
		return
	}
	index := 0
	offset := rf.snapoffset
	// fmt.Printf("me:%v log before sync%v\n", rf.me, rf.logs)
	for i := 0; i < len(args.Log); i++ {
		index = args.Log[i].Index
		// 修改这里赋值log的逻辑，注意不能用nextindex修改，而要用log的长度修改，
		// 因为nextindex和log实际长度不对应会导致错误的append
		if index-offset < len(rf.logs) {
			rf.logs[index-offset] = args.Log[i]
		} else {
			rf.logs = append(rf.logs, args.Log[i])
		}
	}
	rf.matchIndex[rf.me] = index
	rf.nextIndex = index + 1
	// fmt.Printf("me:%v log after sync%v\n", rf.me, rf.logs)
	reply.Flag = true
	fmt.Printf("me:%v sync from%v log%v to %v\n", rf.me, args.Me, args.Log[0].Index, index)

	// 似乎找到了新的优化点，在synclog时，可以把follower的节点commit到和leader一致
	// 本来的写法follower只在下次heartbeat时才commit，在figure83C会出现古怪的错误
	// 对于能commit的节点必须尽快commit
	// 这里有没有需要新term才能commit要求呢？应该是不存在的，只有leader才需要考虑
	// 对于落后的follower来说，进行的commit都是安全的

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
