package raft

import (
	"fmt"
	"time"
)

type LogEntry struct {
	Command interface{}
	Term    int
	// Index   int
}

type CheckMatchLogArgs struct {
	Me   int
	Term int
}

type CheckMatchLogReply struct {
	Me         int
	Term       int
	MatchIndex int
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
	newlog := LogEntry{command, rf.term}
	rf.logs = append(rf.logs, newlog)
	fmt.Printf("me:%v append log, len%v\n", rf.me, len(rf.logs))
	index := len(rf.logs) - 1
	term := rf.term
	rf.mu.Unlock()
	// msg := ApplyMsg{CommandValid: true, CommandIndex: index, Command: command}
	// rf.applyCh <- msg
	// rf.nextIndex++
	go rf.syncLog(term)
	return index, term, true
}

// 对于每次start，启动一个goroutine去同步logs，当一半以上都同步时，commit
func (rf *Raft) syncLog(term int) {
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

}

func (rf *Raft) MatchLog(server int) {
	// 先一个个往前遍历，寻找最后一个同步的节点

	// 找到后，把后面的entrylog一个个传入，保证一致性

	// 传完后，给leader发送信号，表示可以commit
	rf.mu.Lock()
	args := CheckMatchLogArgs{rf.me, rf.term}
	rf.mu.Unlock()
	reply := CheckMatchLogReply{}
	ok := false
	for !ok {
		ok = rf.sendMatchLog(server, &args, &reply)
		time.Sleep(50 * time.Millisecond)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.term {
		// 自己处于低term
		rf.term = reply.Term
		rf.state = StateFollower
		return
	}
	// if rf.state == StateLeader && reply.MatchIndex < rf.matchIndex {
	// 	fmt.Printf("server%v is low matchindex\n", reply.Me)
	// }
}

func (rf *Raft) sendMatchLog(server int, args *CheckMatchLogArgs, reply *CheckMatchLogReply) bool {
	ok := rf.peers[server].Call("Raft.CheckMatchLog", args, reply)
	return ok
}

func (rf *Raft) CheckMatchLog(args *CheckMatchLogArgs, reply *CheckMatchLogReply) {
	// Your code here (3A, 3B).
	reply.Me = rf.me
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if args.Term < reply.Term {
		fmt.Printf("me:%v reci old leader check match,ignore\n", rf.me)
		return
	}
	rf.state = StateFollower
	rf.term = args.Term
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(randomHeartbeatTimeout())
	// reply.MatchIndex = rf.matchIndex
	fmt.Printf("me:%v reply matchindex:%v\n", rf.me, rf.matchIndex)
}
