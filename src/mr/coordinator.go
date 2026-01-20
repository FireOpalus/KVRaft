package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type Coordinator struct {
	// Your definitions here.
	TaskPhase Phase
	NextTaskId int
	TaskChannel chan *Task
	TaskMap map[int]*Task
	ReduceNum int
	MapNum int
	Mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// generate taskid for the new task
//
func (c *Coordinator) GenTaskId() int {
	res := c.NextTaskId
	c.NextTaskId++
	return res
}

//
// generate map tasks
//
func (c *Coordinator) GenMapTask(files []string) {
	fmt.Println("begin make map tasks...")

	for _, file := range files {
		id := c.GenTaskId()
		input := []string{file}
		mapTask := Task{
			TaskId: id,
			TaskType: MapTask,
			TaskState: Ready,
			Files: input,
			ReduceNum: c.ReduceNum,
			StartTime: time.Now().UnixMilli(),
		}
		fmt.Println("make reduce task, taskid:", id)
		c.TaskChannel <- &mapTask
	}
}

//
// generate reduce tasks
//
func (c *Coordinator) GenReduceTask() {
	fmt.Println("begin make reduce tasks...")

	rn := c.ReduceNum

	dir, _ := os.Getwd()
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < rn; i++ {
		id := c.GenTaskId()
		input := []string{}
		
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
				input = append(input, file.Name())
			} 
		}

		reduceTask := Task {
			TaskId: id,
			TaskType: ReduceTask,
			TaskState: Ready,
			Files: input,
			ReduceNum: c.ReduceNum,
			ReduceIdx: id - c.MapNum,
			StartTime: time.Now().UnixMilli(),
		}
		fmt.Println("make reduce task, taskid:", id)
		c.TaskChannel <- &reduceTask
	}
}

//
// check if all the tasks are finished
//
func (c *Coordinator) TaskDone() bool {
	DoneNum, TotalNum := 0, 0
	var TaskType TaskType

	switch c.TaskPhase {
	case MapPhase:
		TotalNum = c.MapNum
		TaskType = MapTask
	case ReducePhase:
		TotalNum = c.ReduceNum
		TaskType = ReduceTask
	}
	for _, v := range c.TaskMap {
		if v.TaskType == TaskType && v.TaskState == Finished {
			DoneNum++
		}
	}
	

	return DoneNum == TotalNum
}

//
// switch phase to next
//
func (c *Coordinator) NextPhase() {
	switch c.TaskPhase {
	case MapPhase: 
		c.TaskPhase = ReducePhase
		c.GenReduceTask()
	case ReducePhase: c.TaskPhase = AllDone
	}
}

//
// RequestTask RPC handler
// the RPC argument and reply types are defined in rpc.go
//
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	if c.TaskPhase == AllDone {
		reply.Answer = TaskFinish
		fmt.Println("all tasks finished")
		return nil
	}

	defer c.Mu.Unlock()
	c.Mu.Lock()
	if len(c.TaskChannel) > 0 {
		taskp := <- c.TaskChannel
		if taskp.TaskState == Ready {
			reply.Answer = TaskGetted
			reply.ResTask = *taskp
			c.TaskMap[taskp.TaskId] = taskp
			taskp.TaskState = Working
			fmt.Printf("Task[%d] has been assigned.\n", taskp.TaskId)
		}
	} else {
		reply.Answer = TaskWait

		if c.TaskDone() {
			c.NextPhase()
		}
	}
	
	return nil
}

//
// when work finish the task, will report to cooridinator
// modify the task state
//
func (c *Coordinator) UpdateTaskState(args *FinishArgs, reply *FinishReply) error {
	id := args.TaskId
	taskp := c.TaskMap[id]
	taskp.TaskState = Finished
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		TaskPhase: MapPhase,
		NextTaskId: 0,
		TaskChannel: make(chan *Task, len(files) + nReduce),
		TaskMap: make(map[int]*Task, len(files) + nReduce),
		ReduceNum: nReduce,
		MapNum: len(files),
	}
	
	c.GenMapTask(files)

	c.server()
	return &c
}
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.TaskPhase == AllDone
}

