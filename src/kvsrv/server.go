package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu          sync.Mutex
	DataBase    map[string]string
	HistoryData map[int64]string
	// TranscationRecord map[int64]map[int]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.DataBase[args.Key]
	// if kv.TranscationRecord[args.ClientId] == nil {
	// 	kv.TranscationRecord[args.ClientId] = make(map[int]string)
	// }
	// kv.TranscationRecord[args.ClientId][args.TranscationID] = "Get"
	// fmt.Printf("Get:%v->%v\n", args.Key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = args.Value
	kv.DataBase[args.Key] = args.Value
	// if kv.TranscationRecord[args.ClientId] == nil {
	// 	kv.TranscationRecord[args.ClientId] = make(map[int]string)
	// }
	// kv.TranscationRecord[args.ClientId][args.TranscationID] = "Put"
	// fmt.Printf("Put:%v->%v\n", args.Key, args.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// value, exists := kv.TranscationRecord[args.ClientId][args.TranscationID]
	// if !exists {
	// 	if kv.TranscationRecord[args.ClientId] == nil {
	// 		kv.TranscationRecord[args.ClientId] = make(map[int]string)
	// 	}
	// 	kv.TranscationRecord[args.ClientId][args.TranscationID] = kv.DataBase[args.Key]
	// 	reply.Value = kv.DataBase[args.Key]
	// 	kv.DataBase[args.Key] += args.Value
	// } else {
	// 	reply.Value = value
	// }

	value, exists := kv.HistoryData[args.ClientId]
	if !exists {
		kv.HistoryData[args.ClientId] = kv.DataBase[args.Key]
		reply.Value = kv.DataBase[args.Key]
		kv.DataBase[args.Key] += args.Value
	} else {
		reply.Value = value
	}

	// reply.Value = kv.DataBase[args.Key]
	// kv.DataBase[args.Key] += args.Value

	// fmt.Printf("Append:%v->%v,Reply:%v\n", args.Key, args.Value, reply.Value)
}

func (kv *KVServer) Report(args *ReportArgs, reply *ReportReply) {
	// kv.TranscationRecord[args.ClientId][args.TranscationID-1] = "Append"
	// kv.TranscationRecord[args.ClientId][args.TranscationID] = "Report"
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.HistoryData, args.ClientId)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.DataBase = make(map[string]string)
	kv.HistoryData = make(map[int64]string)
	// kv.TranscationRecord = make(map[int64]map[int]string)
	return kv
}
