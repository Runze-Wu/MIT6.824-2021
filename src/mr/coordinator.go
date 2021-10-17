package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStat int

const (
	Ready    TaskStat = 0
	Running  TaskStat = 1
	Queueing TaskStat = 2
	Finish   TaskStat = 3
)

const (
	ScheduleInterval = time.Millisecond * 500
	TaskMaxRunTime   = time.Second * 10
)

type TaskStatus struct {
	TaskStat  TaskStat
	StartTime time.Time

	workerId int // debug info
}

type Coordinator struct {
	// Your definitions here.
	MapNum      int // total map tasks' number
	ReduceNum   int // total reduce tasks' number
	WorkerCount int
	TaskRole    TaskRole // all map done turn to reduce
	TaskStatus  []TaskStatus

	files  []string
	mu     sync.Mutex
	done   bool
	taskCh chan Task
}

func (c *Coordinator) getTask(taskId int) Task {
	task := Task{
		c.TaskRole, "", c.MapNum, c.ReduceNum,
		taskId, false}
	if c.TaskRole == Mapper {
		task.MapInput = c.files[taskId]
	}
	return task
}

func (c *Coordinator) run() {
	for !c.Done() {
		c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (c *Coordinator) schedule() {
	curFinish := true
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.TaskStatus); i++ {
		switch c.TaskStatus[i].TaskStat {
		case Ready:
			curFinish = false
			c.TaskStatus[i].TaskStat = Queueing
			c.taskCh <- c.getTask(i)
		case Running:
			curFinish = false
			if time.Since(c.TaskStatus[i].StartTime) > TaskMaxRunTime {
				c.TaskStatus[i].TaskStat = Queueing
				c.taskCh <- c.getTask(i)
			}
		case Queueing:
			curFinish = false
		case Finish: // nothing to do with finished Task
		}
	}
	if curFinish && c.TaskRole == Mapper {
		c.TaskRole = Reducer
		c.TaskStatus = make([]TaskStatus, c.ReduceNum)
	} else if curFinish && c.TaskRole == Reducer {
		c.done = true
	}
}

func (c *Coordinator) turnRole() {
	c.mu.Lock()
	defer c.mu.Unlock()

}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	//log.Println("register called")
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.WorkerId = c.WorkerCount + 1
	c.WorkerCount++
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//log.Println("request called")
	reply.Task = <-c.taskCh
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TaskStatus[reply.Task.TaskId].TaskStat = Running
	c.TaskStatus[reply.Task.TaskId].StartTime = time.Now()
	c.TaskStatus[reply.Task.TaskId].workerId = args.WorkerId
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	//log.Println("report called")
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task.Done {
		c.TaskStatus[args.Task.TaskId].TaskStat = Finish
	} else {
		c.TaskStatus[args.Task.TaskId].TaskStat = Ready
	}
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
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		len(files), nReduce, 0, Mapper,
		make([]TaskStatus, len(files)),
		files, sync.Mutex{}, false, make(chan Task, len(files)),
	}
	if len(files) < nReduce {
		c.taskCh = make(chan Task, nReduce)
	}
	// Your code here.
	go c.run()
	c.server()
	return &c
}
