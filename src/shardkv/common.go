package shardkv

import (
	"fmt"
	"log"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

func DFatal(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Fatal(format, a)
	}
	return
}

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrKilled          = "ErrKilled"
	ErrTermchanged     = "ErrTermchanged"
	ErrNotcommit       = "ErrNotcommit"
	ErrCompleted       = "ErrCompleted"
	YES                = "Y"
	NO                 = "N"
	ErrSyncDBCompleted = "ErrSyncDBCompleted"
)

const (
	GET     = "Get"
	PUT     = "Put"
	APPEND  = "Append"
	SYNC    = "Sync"
	SYNCFIN = "Syncfin"
)

const (
	NOTRESPONSIBLE    = 0
	RESPONSIBLE       = 1
	WAITSYNC          = 2
	SENDSYNCWAITAGREE = 3 // 两种sendsync状态，用于让group内部一致
	SENDSYNC          = 4
)

type Err string

type KVArgs struct {
	ClientID      int64
	TranscationID int
	Type          string
	Key           string
	Shard         int
	Value         string
}

type KVReply struct {
	ServerID int
	Value    string
	Err      Err
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID      int64
	TranscationID int
	Type          string
	Key           string
	Shard         int
	Value         string
}

type OpShell struct {
	Operate *Op
}

type SyncDBArgs struct {
	ServerID int64
	Shard    int
	DB       map[string]string
	HisTran  map[int64]int
}

type SyncDBReply struct {
	ServerID int64
	Err      Err
}
