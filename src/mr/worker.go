package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerTask := new(Task)
	alive := true
	for alive {
		answer := RequestTask(workerTask)
		switch answer {
		case TaskGetted:
			switch workerTask.TaskType {
			case MapTask: DoMapTask(workerTask, mapf)
			case ReduceTask: DoReduceTask(workerTask, reducef)
			}
			alive = RequestFinish(workerTask.TaskId)
		case TaskFinish:
			alive = false
		case TaskWait:
			time.Sleep(time.Millisecond * 500)
		default:
			log.Printf("time out")
			alive = false
		}
	}
}

// 执行 map 任务
// mapf 是在 main/mrworker.go 创建 worker 时传入的函数指针
// workerTask 是在 worker 向 coordinator 请求得到的任务
func DoMapTask(workerTask *Task, mapf func(string, string) []KeyValue) {
	//
	// read each input file
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []KeyValue{}
	for _, filename := range workerTask.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// generate intermediate file
	// file name formation like: mr-X-Y
	// X is the TaskId, Y is value of ihash(key) % nreduce
	//
	rn := workerTask.ReduceNum
	for i := 0; i < rn; i++ {
		iname := "mr-" + strconv.Itoa(workerTask.TaskId) + "-" + strconv.Itoa(i)
		ifile, _ := os.CreateTemp("", "temp*")
		// ifile, _ := os.Create(iname)
		enc := json.NewEncoder(ifile)
		for _, kv := range intermediate {
			if ihash(kv.Key) % rn == i {
				enc.Encode(&kv)
			}
		}
		os.Rename(ifile.Name(), iname)
		ifile.Close()
	}
}

// 执行 Reduce 任务
// reducef 是在 main/mrworker.go 创建 worker 时传入的函数指针
// workerTask 是在 worker 向 coordinator 请求得到的任务
func DoReduceTask(workerTask *Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	intermediate = shuffle(workerTask.Files)

	oname := "mr-out-" + strconv.Itoa(workerTask.ReduceIdx)
	ofile, _ := os.CreateTemp("", "temp*")
	// ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-X.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()
}

// 
// 将多个文件中的 kv 读取并排序
//
func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue

			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	return kva
}

//
// function to make an RPC call to the coordinator for the task
//
// I modified the RPC argument and reply types in rpc.go.
//
func RequestTask(workerTask *Task) TaskResponseFlag {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply) // Need coordinator's function
	if ok {
		if reply.Answer == TaskGetted {
			*workerTask = reply.ResTask
		}
		return reply.Answer
	} 
	return TaskTimeOut
}

//
// function to make an RPC call to the coordinator for the task-finish report
//
func RequestFinish(taskId int) bool {
	args := FinishArgs{TaskId: taskId}
	reply := FinishReply{}

	return call("Coordinator.UpdateTaskState", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("Error dialing RPC: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
