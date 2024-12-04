package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"bytes"
	"fmt"
	"sync"

	"6.5840/labgob"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

func (ps *Persister) OnlySaveRaftstate(raftstate []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
}

func (ps *Persister) OnlySaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = clone(snapshot)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// fmt.Printf("me:%v start persist\n", rf.me)
	e.Encode(rf.term)
	e.Encode(rf.voteTo)
	e.Encode(rf.logs)
	e.Encode(rf.commitIndex)
	e.Encode(rf.snapoffset)
	e.Encode(rf.nextIndex)
	e.Encode(rf.snapshot.LastIndex)
	e.Encode(rf.snapshot.LastTerm)

	rf.persister.OnlySaveRaftstate(w.Bytes())
	// rf.persister.Save(w.Bytes(), rf.snapshot.Data)
	// if rf.snapshot != nil {
	// 	if e.Encode(rf.snapshot.LastIndex) != nil || e.Encode(rf.snapshot.LastTerm) != nil {
	// 		fmt.Printf("me:%v Encode Raft State Failed\n", rf.me)
	// 	}
	// 	rf.persister.Save(w.Bytes(), rf.snapshot.Data)
	// } else {
	// 	rf.persister.Save(w.Bytes(), nil)
	// }
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// fmt.Printf("me:%v read persist\n", rf.me)

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	var Term int
	var Voteto map[int]int
	var Log []LogEntry
	var CommitIndex int
	var Snapoffset int
	var NextIndex int
	var LastIndex int
	var LastTerm int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&Term); err != nil {
		fmt.Printf("me:%v Read term error:%v\n", rf.me, err)
		return
	}
	rf.term = Term

	if err := d.Decode(&Voteto); err != nil {
		fmt.Printf("me:%v Read voteto error:%v\n", rf.me, err)
		return
	}
	rf.voteTo = Voteto

	if err := d.Decode(&Log); err != nil {
		fmt.Printf("me:%v Read log error:%v\n", rf.me, err)
		return
	}
	rf.logs = Log

	if err := d.Decode(&CommitIndex); err != nil {
		fmt.Printf("me:%v Read Commitindex error:%v\n", rf.me, err)
		return
	}
	rf.commitIndex = CommitIndex

	if err := d.Decode(&Snapoffset); err != nil {
		fmt.Printf("me:%v Read snapoffset error:%v\n", rf.me, err)
		return
	}
	rf.snapoffset = Snapoffset

	if err := d.Decode(&NextIndex); err != nil {
		fmt.Printf("me:%v Read nextindex error:%v\n", rf.me, err)
	}
	rf.nextIndex = NextIndex

	if err := d.Decode(&LastIndex); err != nil {
		fmt.Printf("me:%v Read lastindex error:%v\n", rf.me, err)
	}
	rf.snapshot.LastIndex = LastIndex

	if err := d.Decode(&LastTerm); err != nil {
		fmt.Printf("me:%v Read lastterm error:%v\n", rf.me, err)
	}
	rf.snapshot.LastTerm = LastTerm
}
