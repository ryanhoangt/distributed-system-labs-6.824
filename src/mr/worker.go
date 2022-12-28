package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const (
	TaskInterval time.Duration = 200
)

type WorkerInfo struct {
	nReduce int
	task Task
	splitName string
	id int
}

func (worker *WorkerInfo) init(nReduce int, task Task, splitName string) {
	worker.nReduce = nReduce
	worker.task = task
	worker.splitName = splitName
	worker.id = task.Id
}

func (worker *WorkerInfo) doMap(mapf func(string, string) []KeyValue) {
	fileName := worker.splitName
	fileContent, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal("read file error:", err)
	}
	interkv := mapf(fileName, string(fileContent))

	worker.saveToIntermediateFiles(interkv)
}

func (worker *WorkerInfo) saveToIntermediateFiles(kva []KeyValue) {
	prefix := fmt.Sprintf("/tmp/mr-%v", worker.id)
	files := make([]*os.File, 0, worker.nReduce)
	buffers := make([]*bufio.Writer, 0, worker.nReduce)
	encoders := make([]*json.Encoder, 0, worker.nReduce)

	for i := 0; i < worker.nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("cannot create file %v: %v\n", filePath, err)
		}
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}
	
	// fmt.Printf("kva: %v", kva)
	// fmt.Printf("worker info: %v", *worker)

	for _, kv := range kva {
		idx := ihash(kv.Key) % worker.nReduce
		err := encoders[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v to file\n", kv)
		}
	}

	for i, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			log.Fatalf("cannot flush buffer for file: %v\n", files[i].Name())
		}
	}

	// atomatically rename temp files 
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			log.Fatalf("cannot rename file %v\n", file.Name())
		}
	}
}

func (worker *WorkerInfo) doReduce(reducef func(string, []string) string) {
	files, err := filepath.Glob(fmt.Sprintf("/tmp/mr-%v-%v", "*", worker.id))
	if err != nil {
		log.Fatal("cannot list intermediate files")
	}
	
	kToVArr := make(map[string][]string)
	
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open file %v\n", filePath)
		}

		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("cannot decode from file %v\n", filePath)
			}
			kToVArr[kv.Key] = append(kToVArr[kv.Key], kv.Value)
		}
	}

	keys := make([]string, 0, len(kToVArr))
	for k := range kToVArr {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	filePath := fmt.Sprintf("/tmp/mr-out-%v-%v", worker.id, os.Getpid())
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("cannot create file %v\n", filePath)
	}

	for _, k := range keys {
		res := reducef(k, kToVArr[k])
		fmt.Fprintf(file, "%v %v\n", k, res)
		if err != nil {
			log.Fatalf("cannot write mr output (%v, %v) to file", k, res)
		}
	}

	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", worker.id)
	err = os.Rename(filePath, newPath)
	if err != nil {
		log.Fatalf("cannot rename file %v\n", filePath)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := new(WorkerInfo)
	
	for {
		getTaskReply := new(GetTaskReply)
		succ := call("Master.GetTask", &GetTaskArgs{}, &getTaskReply)
		if !succ {
			fmt.Println("Failed to contact master, worker exiting...")
			return
		}

		worker.init(getTaskReply.NReduce, getTaskReply.AssignedTask, getTaskReply.SplitName)

		commitReply := new(CommitTaskReply)
		callSucc := true
		switch worker.task.Type {
		case Exit:
			fmt.Println("All jobs are done, worker exiting...")
			return
		case Map:
			worker.doMap(mapf)
			callSucc = call("Master.CommitTask", 
			&CommitTaskArgs{
				Task{worker.task.Id, worker.task.Type},
				}, &commitReply)
		case Reduce:
			worker.doReduce(reducef)
			callSucc = call("Master.CommitTask", 
			&CommitTaskArgs{
				Task{worker.task.Id, worker.task.Type},
				}, &commitReply)
		default: // NoTask
		}

		if !callSucc {
			fmt.Println("Cannot commit task to master, worker exitting...")
			return
		}

		time.Sleep(time.Millisecond * TaskInterval)
	}


	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
