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
	TaskLock []sync.Mutex
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

	if len(c.TaskChannel) > 0 {
		taskp := <- c.TaskChannel
		c.TaskLock[taskp.TaskId].Lock()
		if taskp.TaskState == Ready {
			reply.Answer = TaskGetted
			reply.ResTask = *taskp
			c.TaskMap[taskp.TaskId] = taskp
			taskp.TaskState = Working
			taskp.StartTime = time.Now()
			fmt.Printf("Task[%d] has been assigned.\n", taskp.TaskId)
		}
		c.TaskLock[taskp.TaskId].Unlock()
	} else {
		reply.Answer = TaskWait
		
		c.Mu.Lock()

		if c.TaskDone() {
			c.NextPhase()
		}

		c.Mu.Unlock()
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

	c.TaskLock[id].Lock()
	defer c.TaskLock[id].Unlock()

	if taskp.TaskState == Working {
		taskp.TaskState = Finished
	}
	return nil
}

//
// when a task working time more than the limit, coordinator will reset the task and redistribute to others
//
func (c *Coordinator) CrashHandler() {
	for {
		if c.TaskPhase == AllDone {
			return
		}
		// if now - start > 10s, reset task
		for _, task := range c.TaskMap {

			c.TaskLock[task.TaskId].Lock()

			if task.TaskState == Working && time.Since(task.StartTime) > time.Second * 10 {
				task.TaskState = Ready
				c.TaskChannel <- task
				delete(c.TaskMap, task.TaskId)
			}

			c.TaskLock[task.TaskId].Unlock()
		}

		time.Sleep(time.Second * 3)
	}
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
		TaskLock: make([]sync.Mutex, len(files) + nReduce),
	}
	
	c.GenMapTask(files)

	go c.CrashHandler()

	c.server()
	return &c
}
//
func (c *Coordinator) Done() bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	// Your code here.
	return c.TaskPhase == AllDone
}

