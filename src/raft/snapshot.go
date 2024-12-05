package raft

import (
	"fmt"
)

type Snapshot struct {
	LastIndex int
	LastTerm  int

	Data []byte
}

type InstallSnapshotArgs struct {
	Me          int
	CurTerm     int
	InstallSnap Snapshot
}

type InstallSnapshotReply struct {
	Me         int
	CurTerm    int
	Flag       bool
	IfOutedate bool
}

// snapshot有[]byte,所以需要深拷贝
// interface{}进行深拷贝有点复杂，还是不传递interface
// 这样的结果是follower在logs[0]对应的command数据可能是错误的。
// 应对策略是在snapshot之外避免对logs[0]进行操作，包括取log同步等
func DeepCopySnap(origin *Snapshot) Snapshot {
	copySnap := Snapshot{origin.LastIndex, origin.LastTerm, nil}
	if origin.Data != nil {
		copySnap.Data = make([]byte, len(origin.Data))
		copy(copySnap.Data, origin.Data)
	}
	return copySnap
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

func (rf *Raft) InstallSnapshot(server int, startterm int) {
	rf.rwmu.Lock()
	if rf.state != StateLeader || startterm != rf.term {
		rf.rwmu.Unlock()
		return
	}
	term := rf.term
	rf.ininstallsnap[server] = true
	snap := DeepCopySnap(rf.snapshot)
	args := InstallSnapshotArgs{rf.me, term, snap}
	reply := InstallSnapshotReply{}
	rf.rwmu.Unlock()

	fmt.Printf("me:%v in term%v send snapshot{%v %v} to %v\n", rf.me, term, snap.LastIndex, snap.LastTerm, server)
	ok := false
	rpccount := 0
	for !ok {
		ok = rf.sendSnapshot(server, &args, &reply)
		if reply.IfOutedate {
			rf.rwmu.Lock()
			defer rf.rwmu.Unlock()
			rf.ininstallsnap[server] = false
			if rf.term < reply.CurTerm {
				rf.term = reply.CurTerm
				rf.TurntoFollower()
				rf.persist()
			}
			return
		}
		rpccount++
		if rpccount > 3 {
			rf.rwmu.Lock()
			rf.ininstallsnap[server] = false
			rf.rwmu.Unlock()
			return
		}
	}

	// 检查自己是否过时了
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	rf.ininstallsnap[server] = false
	if rf.term != term || rf.state != StateLeader {
		return
	}
	if rf.term < reply.CurTerm {
		fmt.Printf("me:%v is old term, change to follower\n", rf.me)
		rf.term = reply.CurTerm
		rf.TurntoFollower()
		rf.persist()
		return
	}
	if reply.Flag && rf.matchIndex[server] < snap.LastIndex {
		rf.matchIndex[server] = snap.LastIndex
		fmt.Printf("me:%v in InstallSnapshot, change matchindex[%v]=%v\n", rf.me, server, snap.LastIndex)
		// 这里不应该进行commit确认，因为靠install snapshot, matchindex最多只能到rf.commitindex的位置，无法进一步commit
		// rf.commiter()
	}
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.FollowerInstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) FollowerInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Me = rf.me
	reply.CurTerm = args.CurTerm
	reply.Flag = false
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	if args.CurTerm < rf.term {
		// old request , refuse
		fmt.Printf("me:%v reci old leader install snapshot, ignore\n", rf.me)
		reply.CurTerm = rf.term
		reply.IfOutedate = true
	} else {
		if args.CurTerm > rf.term {
			rf.term = args.CurTerm
			rf.persist()
		}
		rf.TurntoFollower()
	}
	if args.InstallSnap.LastIndex <= rf.snapshot.LastIndex {
		fmt.Printf("me:%v reci old snapshot, do not install\n", rf.me)
		return
	}
	// 传来的snapshot比已有的更新，就进行替换和log压缩
	snap := DeepCopySnap(&args.InstallSnap)
	rf.snapshot = &snap
	if snap.LastIndex < rf.nextIndex {
		rf.logs = rf.logs[snap.LastIndex-rf.snapoffset:]
	} else {
		rf.logs = make([]LogEntry, 1)
		rf.logs[0].Index = snap.LastIndex
		rf.logs[0].Term = snap.LastTerm
		rf.nextIndex = snap.LastIndex + 1
	}
	rf.snapoffset = snap.LastIndex

	reply.Flag = true
	if snap.LastIndex <= rf.commitIndex {
		fmt.Printf("me:%v snapshot is older than commmit index\n", rf.me)
	} else {
		fmt.Printf("me:%v newer snapshot%v, commit it\n", rf.me, snap.LastIndex)
		fmt.Printf("me:%v commit log[%v]-log[%v] func:FollowerInstallSnapshot\n", rf.me, rf.commitIndex, snap.LastIndex)
		msg := ApplyMsg{SnapshotValid: true, Snapshot: snap.Data, SnapshotIndex: snap.LastIndex, SnapshotTerm: snap.LastTerm}
		rf.applyCh <- msg
		rf.commitIndex = snap.LastIndex
	}
	rf.persist()
	rf.persister.OnlySaveSnapshot(rf.snapshot.Data)
}
