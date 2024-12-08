package shardctrler

import "fmt"

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
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
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type Err string

type JoinArgs struct {
	ClientID      int64
	TranscationID int
	Type          string
	Servers       map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	ServerID int
	Err      Err
}

type LeaveArgs struct {
	ClientID      int64
	TranscationID int
	Type          string
	GIDs          []int
}

type LeaveReply struct {
	ServerID int
	Err      Err
}

type MoveArgs struct {
	ClientID      int64
	TranscationID int
	Type          string
	Shard         int
	GID           int
}

type MoveReply struct {
	ServerID int
	Err      Err
}

type QueryArgs struct {
	ClientID      int64
	TranscationID int
	Type          string
	Num           int // desired config number
}

type QueryReply struct {
	ServerID int
	Err      Err
	Config   Config
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

type Args interface {
	Print()
	getType() string
}

type Reply interface {
	Print()
	getErr() Err
}

func (args *JoinArgs) Print() {
	DPrintf("JoinArgs:%v\n", args)
}

func (args *JoinArgs) getType() string {
	return args.Type
}

func (reply *JoinReply) Print() {
	DPrintf("JoinReply:%v\n", reply)
}

func (reply *JoinReply) getErr() Err {
	return reply.Err
}

func (args *LeaveArgs) Print() {
	DPrintf("LeaveArgs:%v\n", args)
}

func (args *LeaveArgs) getType() string {
	return args.Type
}

func (reply *LeaveReply) Print() {
	DPrintf("LeaveReply:%v\n", reply)
}

func (reply *LeaveReply) getErr() Err {
	return reply.Err
}

func (args *MoveArgs) Print() {
	DPrintf("MoveArgs:%v\n", args)
}

func (args *MoveArgs) getType() string {
	return args.Type
}

func (reply *MoveReply) Print() {
	DPrintf("MoveReply:%v\n", reply)
}

func (reply *MoveReply) getErr() Err {
	return reply.Err
}

func (args *QueryArgs) Print() {
	DPrintf("QueryArgs:%v\n", args)
}

func (args *QueryArgs) getType() string {
	return args.Type
}

func (reply *QueryReply) Print() {
	DPrintf("QueryReply:%v\n", reply)
}

func (reply *QueryReply) getErr() Err {
	return reply.Err
}
