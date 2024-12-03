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

type SyncLogEntryArgs struct {
	Me      int
	CurTerm int
	Log     []LogEntry
}

type SyncLogEntryReply struct {
	Me         int
	CurTerm    int
	Flag       bool // 返回是否同步成功
	IfOutedate bool
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
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	state := rf.state
	term := rf.term
	index := rf.nextIndex

	if state != StateLeader {
		return -1, -1, false
	}
	newlog := LogEntry{command, term, index}

	rf.matchIndex[rf.me] = index
	if len(rf.logs) > index {
		rf.logs[index] = newlog
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

func (rf *Raft) commiter() {
	for !rf.killed() {
		time.Sleep(200 * time.Millisecond)
		// 先考虑不降级leader的情况
		// fmt.Printf("me:%v every0.2s check if commit\n", rf.me)
		// 这里问题出现在哪里呢？因为这里出现释放锁后又重复获取的情况，在之前判定通过后
		// 锁被别的线程取走占用了很长时间，而这段时间rf已经不再是leader了，却没有进行判定
		// 简单的放在一起即可，每次commit时全程占有锁
		if rf.killed() {
			// fmt.Printf("me:%v killed, stop commiter\n", rf.me)
			return
		}
		rf.rwmu.Lock()
		fmt.Printf("me:%v every0.2s check if commit\n", rf.me)
		if rf.state != StateLeader {
			fmt.Printf("me:%v not leader return\n", rf.me)
			rf.rwmu.Unlock()
			return
		}
		rf.matchIndex[rf.me] = rf.nextIndex - 1
		matchIndex := make([]int, rf.serverNum)
		copy(matchIndex, rf.matchIndex)
		sort.Ints(matchIndex)
		fmt.Printf("matchindex:%v\n", rf.matchIndex)
		if rf.commitIndex >= matchIndex[rf.serverNum/2] {
			fmt.Printf("no new commits\n")
			rf.rwmu.Unlock()
			continue
		}
		index := matchIndex[rf.serverNum/2]
		if rf.logs[index].Term < rf.term {
			fmt.Printf("new match log%v, but old term%v not commit\n", rf.logs[index], rf.term)
			rf.rwmu.Unlock()
			continue
		}
		i := 0
		for rf.commitIndex < matchIndex[rf.serverNum/2] {
			rf.commitIndex++
			msg := ApplyMsg{CommandValid: true, CommandIndex: rf.commitIndex, Command: rf.logs[rf.commitIndex].Command}
			rf.applyCh <- msg
			fmt.Printf("me:%v once commit one new log %v\n", rf.me, rf.commitIndex)
			i++
			if i == 100 {
				break
			}
		}
		rf.persist()
		rf.rwmu.Unlock()
	}
}

func (rf *Raft) MatchLog(server int, slogentry []SimpleLogEntry, startindex int) {
	// 先一个个往前遍历，寻找最后一个同步的节点
	// leader向follower从后往前发送index与term，找到第一个相同的。
	rf.rwmu.Lock()
	rf.incheck[server] = true
	term := rf.term
	index := startindex
	i := 0
	for i = 0; i < len(slogentry); i++ {
		if slogentry[i].Term != rf.logs[index+i].Term {
			break
		}
	}
	index = index + i
	i = index
	logentries := make([]LogEntry, rf.nextIndex-index)
	for index < rf.nextIndex {
		logentries[index-i] = rf.logs[index]
		index++
	}
	args := SyncLogEntryArgs{rf.me, term, logentries}
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
				ResetTimer(rf.heartbeatTimer, 200, 150)
				if rf.state != StateFollower {
					rf.state = StateFollower
					ResetTimer(rf.voteTimer, 5, 1)
					ResetTimer(rf.leaderTimer, 5, 1)
				}
				rf.persist()
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
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
			ResetTimer(rf.heartbeatTimer, 200, 150)
			ResetTimer(rf.leaderTimer, 5, 1)
			ResetTimer(rf.voteTimer, 5, 1)
		}
		rf.persist()
		return
	}
	if reply.Flag && rf.matchIndex[server] != index-1 {
		rf.matchIndex[server] = index - 1
		fmt.Printf("in matchlog, change matchindex[%v]=%v\n", server, index-1)
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
	if args.CurTerm < rf.term {
		// old request , refuse
		fmt.Printf("me:%v reci old leader check match,ignore\n", rf.me)
		reply.CurTerm = rf.term
		return
	}
	if args.CurTerm > rf.term {
		rf.term = args.CurTerm
		rf.persist()
	}
	if rf.state != StateFollower {
		rf.state = StateFollower
		ResetTimer(rf.voteTimer, 5, 1)
		ResetTimer(rf.leaderTimer, 5, 1)
	}
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
	reply.Flag = true
	fmt.Printf("me:%v sync from%v log%v to %v\n", rf.me, args.Me, args.Log[0].Index, index)
}
