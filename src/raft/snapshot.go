package raft

import (
	"fmt"
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
	// 这个函数由test调用，不需要在代码里显式使用它，只需要实现删除log和persist的逻辑

	// 使用chan commit时，必须占有锁，而测试代码执行的逻辑是commit时转到config.go去执行检查代码
	// 由于还没释放lock，所以代码被阻塞于此，必须启动一个goroutine去执行snapshot
	go rf.CallSnapshot(index, snapshot)
}

func (rf *Raft) CallSnapshot(index int, snapshot []byte) {
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()

	// 因为要等待获取锁，所以可能会是乱序的snapshot，需要检查，保证snapshot更新
	if index <= rf.snapshot.LastIndex {
		fmt.Printf("old snapshot index%v, ignore\n", index)
		return
	}
	// 把snapshot拷贝到rf.snapshot里
	offset := rf.snapoffset
	snap := &Snapshot{}
	snap.LastIndex = index
	// 这里居然忘记也加offset了。。
	snap.LastTerm = rf.logs[index-offset].Index
	snap.Data = make([]byte, len(snapshot))
	copy(snap.Data, snapshot)
	rf.snapshot = snap

	// 删除多余的log, 这里把最后一个index作为新的log[0]，这样可以从log[0]处读取snapshot的index和term
	rf.logs = rf.logs[index-offset:]
	rf.snapoffset = index
	// 有offset后，需要对原有逻辑都进行调整，需要注意锁释放获取前后，snapshot变化的情况
	rf.persist()
	rf.persister.OnlySaveSnapshot(rf.snapshot.Data)
	fmt.Printf("me:%v snapshot to index%v\n", rf.me, index)
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	snap := &Snapshot{}
	snap.LastIndex = rf.snapshot.LastIndex
	snap.LastTerm = rf.snapshot.LastTerm
	snap.Data = make([]byte, len(data))
	copy(snap.Data, data)
	rf.snapshot = snap
}
