package kvsrv

// Put or Append
type PutAppendArgs struct {
	ClientId int64
	// TranscationID int
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	ClientId int64
	// TranscationID int
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}

type ReportArgs struct {
	ClientId int64
	// TranscationID int
}
type ReportReply struct {
}
