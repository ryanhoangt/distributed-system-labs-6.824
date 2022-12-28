package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int
const (
	_ TaskType = iota
	Map 
	Reduce
	NoTask
	Exit
	MAX_N_MAP = 100
)

type Task struct {
	Id int
	Type TaskType
}

type Master struct {
	// Your definitions here.
	nReduceTasks int
	splits []string
	mapTasksCh chan Task
	reduceTasksCh chan Task
	mapTasksDone map[int]bool
	reduceTasksDone map[int]bool
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	switch {
	case m.Done():
		reply.AssignedTask = Task{-1, Exit}
	case m.isAllMapTasksDone():
		reply.NReduce = m.nReduceTasks
		reply.SplitName = ""

		select {
		case reply.AssignedTask = <- m.reduceTasksCh: // there are available tasks to assign
			go m.waitForWorker(reply.AssignedTask)
		default:
			reply.AssignedTask = Task{-1, NoTask}
		}
	default:
		reply.NReduce = m.nReduceTasks
		
		select {
		case reply.AssignedTask = <- m.mapTasksCh:
			reply.SplitName = m.splits[reply.AssignedTask.Id]

			go m.waitForWorker(reply.AssignedTask)
		default:
			reply.AssignedTask = Task{-1, NoTask}
		}
	}
	
	return nil
}

func (m *Master) waitForWorker(task Task) {
	time.Sleep(10 * time.Second)

	switch task.Type {
	case Map:
		if _, isDone := m.mapTasksDone[task.Id]; !isDone {
			m.mapTasksCh <- task
		}
	case Reduce:
		if _, isDone := m.reduceTasksDone[task.Id]; !isDone {
			m.reduceTasksCh <- task
		}
	default:
	}
}

func (m *Master) CommitTask(args *CommitTaskArgs, reply *CommitTaskReply) error {
	taskToCommit := args.ToCommitTask
	fmt.Printf("task %v is committed\n", taskToCommit)

	m.mu.Lock()
	defer m.mu.Unlock()
	if taskToCommit.Type == Map {
		m.mapTasksDone[taskToCommit.Id] = true
	} else {
		m.reduceTasksDone[taskToCommit.Id] = true
	}

	return nil
}


func (m *Master) isAllMapTasksDone() bool {
	return len(m.mapTasksDone) >= len(m.splits)
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
	ret = len(m.reduceTasksDone) >= m.nReduceTasks

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
	m.mapTasksCh = make(chan Task, MAX_N_MAP)
	m.reduceTasksCh = make(chan Task, m.nReduceTasks)
	m.mapTasksDone = make(map[int]bool)
	m.reduceTasksDone = make(map[int]bool)

	for i := range m.splits {
		m.mapTasksCh <- Task{i, Map}
	}

	for i := 0; i < m.nReduceTasks; i++ {
		m.reduceTasksCh <- Task{i, Reduce}
	}

	m.server()
	return &m
}
