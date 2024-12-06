package kvraft

import (
	"fmt"
	"log"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrKilled      = "ErrKilled"
	ErrTermchanged = "ErrTermchanged"
	ErrNotcommit   = "ErrNotcommit"
	ErrCompleted   = "ErrCompleted"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	REPORT = "Report"
)

type Err string

type KVArgs struct {
	ClientID      int64
	TranscationID int
	Type          string
	Key           string
	Value         string
}

type KVReply struct {
	ServerID int
	Value    string
	Err      Err
}

func (args *KVArgs) Print() {
	fmt.Printf("Args Client:%v Trans:%v Type:%v Key%v Value%v\n", args.ClientID, args.TranscationID, args.Type, args.Key, args.Value)
}

func (reply *KVReply) Print() {
	fmt.Printf("Reply Server:%v Value:%v Err:%v\n", reply.ServerID, reply.Value, reply.Err)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID      int64
	TranscationID int
	Type          string
	Key           string
	Value         string
	Status        Err
}
