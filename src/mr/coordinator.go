package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusIdle         = 0
	TaskStatusProcessing   = 1
	TaskStatusAccomplished = 2
)

const (
	TaskTypeMap    = 0
	TaskTypeReduce = 1
	UnAssgined     = 2
)

// // Task任务数据
// type Task struct {
// 	Status     int
// 	TaskType   int
// 	Filename   string
// 	AssignTime time.Time
// }

type Coordinator struct {
	// Your definitions here.
	NumReduce               int
	NumMap                  int
	NumWorker               int
	TaskLock                sync.Mutex
	MapTasks                []Task
	ReduceTasks             []Task
	allMapTasksAccomplished bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// worker注册函数，主要是向worker传参map数和reduce数
func (c *Coordinator) CreateWorker(args *ExampleArgs, reply *Worker_) error {
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()

	reply.WorkerID = c.NumWorker
	reply.NumMap = c.NumMap
	reply.NumReduce = c.NumReduce
	c.NumWorker++
	// fmt.Printf("call createworker,Numworker:%v\n", c.NumWorker)
	return nil
}

// 分配任务的 RPC 处理函数
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()

	// 检查是否有未完成的 Map 任务
	if !c.checkAllTasksCompleted(TaskTypeMap) {
		for i := range c.MapTasks {
			task := &c.MapTasks[i]
			if task.Status == TaskStatusIdle || (task.Status == TaskStatusProcessing && time.Since(task.AssignTime) > 10*time.Second) {
				task.Status = TaskStatusProcessing
				task.AssignTime = time.Now()
				*reply = *task
				return nil
			}
		}
		// for _, task := range c.MapTasks {
		// 	fmt.Printf("Taskstatus:%v, Tasktype:%v,Filename:%v,Time:%v\n", task.Status, task.TaskType, task.Filename, task.AssignTime)
		// }
		reply.TaskType = UnAssgined
		return nil
	} else if !c.checkAllTasksCompleted(TaskTypeReduce) { // 分配reduce
		for i := range c.ReduceTasks {
			task := &c.ReduceTasks[i]
			if task.Status == TaskStatusIdle || (task.Status == TaskStatusProcessing && time.Since(task.AssignTime) > 10*time.Second) {
				task.Status = TaskStatusProcessing
				task.AssignTime = time.Now()
				*reply = *task
				return nil
			}
		}
		reply.TaskType = UnAssgined
		return nil
	}

	// // 如果 Map 任务都完成了，分配 Reduce 任务
	// if c.allMapTasksAccomplished {
	// 	for i := range c.ReduceTasks {
	// 		task := &c.ReduceTasks[i]
	// 		if task.Status == TaskStatusIdle || (task.Status == TaskStatusProcessing && time.Since(task.AssignTime) > 10*time.Second) {
	// 			task.Status = TaskStatusProcessing
	// 			task.AssignTime = time.Now()
	// 			*reply = *task
	// 			return nil
	// 		}
	// 	}
	// }

	// 没有任务可分配，2种情况，一种是任务都完成了，另一种是未完成的任务在进行中，都返回空任务给worker
	reply.Status = TaskStatusAccomplished
	return nil
}

// worker完成任务后向coordinate通知，更新任务状态
func (c *Coordinator) TaskDone(args *Task, reply *ExampleReply) error {
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()

	if args.TaskType == TaskTypeMap {
		if !(time.Since(args.AssignTime) > 10*time.Second || c.MapTasks[args.TaskID].Status == TaskStatusAccomplished) {
			c.MapTasks[args.TaskID].Status = TaskStatusAccomplished
		}
	} else if args.TaskType == TaskTypeReduce {
		if !(time.Since(args.AssignTime) > 10*time.Second || c.ReduceTasks[args.TaskID].Status == TaskStatusAccomplished) {
			c.ReduceTasks[args.TaskID].Status = TaskStatusAccomplished
		}
	}
	return nil
}

// 检查是否所有指定类型的任务已完成
func (c *Coordinator) checkAllTasksCompleted(taskType int) bool {
	var tasks []Task
	if taskType == TaskTypeMap {
		tasks = c.MapTasks
	} else {
		tasks = c.ReduceTasks
	}

	for _, task := range tasks {
		if task.Status != TaskStatusAccomplished {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()

	ret = c.checkAllTasksCompleted(TaskTypeReduce)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:               nReduce,
		NumMap:                  len(files),
		NumWorker:               0,
		MapTasks:                make([]Task, len(files)),
		ReduceTasks:             make([]Task, nReduce),
		allMapTasksAccomplished: false,
	}

	// Your code here.
	// 初始化 Map 任务
	for i, filename := range files {
		c.MapTasks[i] = Task{
			TaskID:   i,
			Status:   TaskStatusIdle,
			TaskType: TaskTypeMap,
			Filename: filename,
		}
	}

	// 初始化 Reduce 任务
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			TaskID:   i,
			Status:   TaskStatusIdle,
			TaskType: TaskTypeReduce,
		}
	}
	// fmt.Printf("NumReduce:%v,NumMap:%v\n", c.NumReduce, c.NumMap)
	c.server()
	return &c
}
