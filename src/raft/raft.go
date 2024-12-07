package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	StateFollower  = 0
	StateCandidate = 1
	StateLeader    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	rwmu      sync.RWMutex        // 给term准备读写锁，提高并发性
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term           int
	state          int
	serverNum      int
	heartbeatTimer *time.Timer
	voteTimer      *time.Timer
	leaderTimer    *time.Timer
	voteTo         map[int]int          //这一轮投票的对象,如果是-1,说明还没投票
	voteGets       map[int]map[int]bool //这一轮获取的投票个数
	voteDisagree   map[int]map[int]bool //这一轮获取的反对票个数，同样用于快速结束选举
	ifstopvote     bool                 //是否停止选票，用来防止过多timer到时提醒

	applyCh     chan ApplyMsg
	logs        []LogEntry // 存的logs
	commitIndex int        //对于leader，表示自己已经提交到了第几个log
	matchIndex  []int      //每个节点要存储与其他节点一致的log下标，但是只有当这个节点是leader时才有用
	nextIndex   int        //下一个要写入的下标
	incheck     []bool     // 这个用来表示某个follower是否正在check中，避免心跳重复执行checklog

	snapshot      *Snapshot
	snapoffset    int    // 用来记录snapshot的偏移量
	ininstallsnap []bool //用来记录某个server是否正在下载snapshot
}

func ResetTimer(t *time.Timer, a int32, b int32) {
	if !t.Stop() {
		// 如果 `Stop` 返回 false，说明 `Timer` 的信号已经进入管道，清空它
		select {
		case <-t.C:
		default:
		}
	}
	ms := a + (rand.Int31() % b)
	t.Reset(time.Duration(ms) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.rwmu.RLock()
	defer rf.rwmu.RUnlock()
	// var term int
	// var isleader bool
	// // Your code here (3A).
	return rf.term, rf.state == StateLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	// Your initialization code here (3A, 3B, 3C).
	rf.serverNum = len(peers)
	rf.term = 0
	rf.state = StateFollower
	rf.voteTimer = time.NewTimer(10000 * time.Millisecond)
	rf.voteTimer.Stop()
	rf.heartbeatTimer = time.NewTimer(10000 * time.Millisecond)
	rf.heartbeatTimer.Stop()
	rf.leaderTimer = time.NewTimer(10000 * time.Millisecond)
	rf.leaderTimer.Stop()
	rf.voteTo = make(map[int]int)
	rf.voteGets = make(map[int]map[int]bool)
	rf.voteDisagree = make(map[int]map[int]bool)

	rf.applyCh = applyCh
	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.commitIndex = 0
	rf.matchIndex = make([]int, rf.serverNum)
	rf.nextIndex = 1
	rf.incheck = make([]bool, rf.serverNum)

	rf.snapoffset = 0
	rf.snapshot = &Snapshot{0, 0, nil}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	rf.ininstallsnap = make([]bool, rf.serverNum)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
