package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskType int
const (
	_ TaskType = iota
	Map 
	Reduce
	MAX_N_MAP = 100
)

type Task struct {
	Id int
	Type TaskType
}

type Master struct {
	// Your definitions here.
	nReduceTasks int
	// workersAddr []net.IP
	splits []string
	tasksCh chan Task // buffered task channel for both Map and Reduce tasks


}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	reply.NReduce = m.nReduceTasks
	reply.AssignedTask = <- m.tasksCh
	reply.SplitName = m.splits[reply.AssignedTask.Id]

	// TODO:
	// - assign no work reply back when task channel is empty
	
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduceTasks = nReduce
	m.splits = files
	m.tasksCh = make(chan Task, MAX_N_MAP + m.nReduceTasks)

	for i := range m.splits {
		m.tasksCh <- Task{i, Map}
	}

	for i := 0; i < m.nReduceTasks; i++ {
		m.tasksCh <- Task{i, Reduce}
	}

	m.server()
	return &m
}
