package raft

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
)

type Snapshot struct {
	LastIndex int
	LastTerm  int

	Data []byte
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	var SnapLastIndex int
	var SnapLastTerm int
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&SnapLastIndex); err != nil {
		fmt.Printf("me:%v Read snaplastindex error:%v\n", rf.me, err)
		return
	}
	rf.snapshot.LastIndex = SnapLastIndex

	if err := d.Decode(&SnapLastTerm); err != nil {
		fmt.Printf("me:%v Read snaplastterm error:%v\n", rf.me, err)
		return
	}
	rf.snapshot.LastTerm = SnapLastTerm
}
