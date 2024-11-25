package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := Worker_{}
	call("Coordinator.CreateWorker", &ExampleArgs{}, &w)
	// fmt.Printf("workerID:%v,nummap:%v,numreduce:%v\n", w.WorkesrID, w.NumMap, w.NumReduce)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		// 向 Coordinator 请求任务
		task := Task{}
		if !call("Coordinator.AssignTask", &ExampleArgs{}, &task) {
			fmt.Println("Failed to request task from Coordinator.")
			return
		}
		// fmt.Printf("Taskstatus:%v, Tasktype:%v,Filename:%v,Time:%v\n", task.Status, task.TaskType, task.Filename, task.AssignTime)
		// 检查是否任务已完成
		if task.Status == TaskStatusAccomplished {
			fmt.Println("All tasks are completed.")
			break
		}

		// 根据任务类型执行相应的 Map 或 Reduce 操作
		switch task.TaskType {
		case TaskTypeMap:
			w.handleMapTask(&task, mapf)
			// 通知 Coordinator 当前任务已完成
			call("Coordinator.TaskDone", &task, &ExampleReply{})
		case TaskTypeReduce:
			w.handleReduceTask(&task, reducef)
			call("Coordinator.TaskDone", &task, &ExampleReply{})
		case UnAssgined:
			// time.Sleep(time.Second)
		}
		// fmt.Printf("Taskstatus:%v, Tasktype:%v,Filename:%v,Time:%v\n", task.Status, task.TaskType, task.Filename, task.AssignTime)

		// 防止 Worker 过于频繁请求任务，适当等待
		time.Sleep(time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// 处理 Map 任务
func (w *Worker_) handleMapTask(task *Task, mapf func(string, string) []KeyValue) {
	// 读取输入文件内容
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("Cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", task.Filename)
	}
	file.Close()

	// 执行 Map 函数
	kva := mapf(task.Filename, string(content))

	// 创建中间文件，分区写入
	partitionFiles := make(map[int]*os.File)
	encoders := make(map[int]*json.Encoder)

	for _, kv := range kva {
		// 计算分区号
		partition := ihash(kv.Key) % w.NumReduce
		// 打开或创建分区文件
		if _, ok := partitionFiles[partition]; !ok {
			// filename := fmt.Sprintf("mr-%d-%d-%d", task.TaskID, partition, w.WorkerID)
			filename := fmt.Sprintf("mr-%d-%d", task.TaskID, partition)
			partitionFiles[partition], err = os.Create(filename)
			if err != nil {
				log.Fatalf("Cannot create file %v", filename)
			}
			encoders[partition] = json.NewEncoder(partitionFiles[partition])
		}

		// 写入分区文件
		err := encoders[partition].Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot encode key-value %v", kv)
		}
	}

	// 关闭所有分区文件
	for _, file := range partitionFiles {
		file.Close()
	}
}

// 处理 Reduce 任务
func (w *Worker_) handleReduceTask(task *Task, reducef func(string, []string) string) {
	// 收集和读取分区文件
	kvMap := make(map[string][]string)
	for i := 0; i < w.NumMap; i++ {
		// pattern := fmt.Sprintf("mr-%d-%d-*", i, task.TaskID)
		// filename, _ := filepath.Glob(pattern)
		// file, err := os.Open(filename[0])
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		file, err := os.Open(filename)

		if err != nil {
			if os.IsNotExist(err) {
				continue
			} else {
				log.Fatalf("Cannot open %v", filename)
			}
			// log.Fatalf("Cannot open %v", filename)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		file.Close()
	}

	// 执行 Reduce 函数
	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Cannot create %v", oname)
	}

	// 检查是否收集到任何数据，如果没有则创建一个空文件
	if len(kvMap) == 0 {
		// 无需写入任何内容，直接关闭文件即可
		ofile.Close()
		return
	}

	for key, values := range kvMap {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	ofile.Close()
}
