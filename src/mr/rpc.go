package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int
type TaskState int
type TaskResponseFlag int

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	Ready TaskState = iota				// Task 未分配，准备分配给 worker
	Working								// Task 已经分配给 worker，未完成
	Finished							// Task 完成
)

const (
	TaskGetted TaskResponseFlag = iota	// 成功得到一个 Task
	TaskWait							// 目前没有空闲的 Task
	TaskFinish							// 所有 Task 已经完成，可以退出
	TaskTimeOut							// Task 相关请求超时
)

type Task struct {
	TaskId int 							// Task 的唯一标识符
	TaskType TaskType
	TaskState TaskState
	Files []string
	ReduceNum int						// nReduce 用于 Map 任务 ihash 输出中间文件
	ReduceIdx int						// idx 用于标明该 Reduce 任务编号
	StartTime time.Time
	WorkerId int
}

// Request for Task
type TaskArgs struct {
	WorkerId int
}					// worker 不需要向 coordinator 传输任何数据

type TaskReply struct {
	Answer TaskResponseFlag
	ResTask Task
}

// Request for Task complete
type FinishArgs struct {
	TaskId int
	WorkerId int
}

type FinishReply struct {}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
