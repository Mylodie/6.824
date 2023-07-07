package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	nReduce int
	files   []string
	imFiles []string
	tasks   []*task

	pollingCh chan *pollingMsg
	reportCh  chan *reportMsg

	doneMapCh  chan struct{}
	doneReduce chan struct{}
}

type taskStatus int

const (
	InitializedTask = iota
	ScheduledTask
	FinishedTask
)

type task struct {
	mapKey    string // file name
	reduceKey int    // hash(key) % NReduce
	status    taskStatus
	startTime time.Time
}

type pollingMsg struct {
	response *PollingResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) DoPoll(_ *PollingRequest, response *PollingResponse) error {
	msg := &pollingMsg{response: response, ok: make(chan struct{})}
	c.pollingCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) DoReport(request *ReportRequest, _ *ReportResponse) error {
	msg := &reportMsg{request: request, ok: make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) allocTaskWithFunc(isValid func(t *task) bool) *task {
	for _, t := range c.tasks {
		if isValid(t) {
			return t
		}
	}
	return nil
}

func (c *Coordinator) scheduleMap() {
	defer close(c.doneMapCh)

	c.tasks = []*task{}
	for _, file := range c.files {
		c.tasks = append(c.tasks, &task{mapKey: file, status: InitializedTask})
	}

	for c.allocTaskWithFunc(func(t *task) bool {
		return t.status != FinishedTask
	}) != nil {
		select {
		case msg := <-c.pollingCh:
			t := c.allocTaskWithFunc(func(t *task) bool {
				return t.status == InitializedTask ||
					t.status == ScheduledTask && time.Since(t.startTime) > time.Second*10
			})
			if t != nil {
				t.startTime = time.Now()
				t.status = ScheduledTask
				msg.response.TaskType = MapType
				msg.response.MapKey = t.mapKey
			} else {
				msg.response.TaskType = IdleType
			}
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			if msg.request.TaskType == MapType {
				t := c.allocTaskWithFunc(func(t *task) bool {
					return t.mapKey == msg.request.MapKey
				})
				if t != nil && t.status == ScheduledTask {
					c.imFiles = append(c.imFiles, msg.request.ImFile)
					t.status = FinishedTask
				}
			}
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) scheduleReduce() {
	defer close(c.doneReduce)

	<-c.doneMapCh
	c.tasks = []*task{}
	for i := 0; i < c.nReduce; i++ {
		c.tasks = append(c.tasks, &task{reduceKey: i, status: InitializedTask})
	}
	for c.allocTaskWithFunc(func(t *task) bool {
		return t.status != FinishedTask
	}) != nil {
		select {
		case msg := <-c.pollingCh:
			t := c.allocTaskWithFunc(func(t *task) bool {
				return t.status == InitializedTask ||
					t.status == ScheduledTask && time.Since(t.startTime) > time.Second*10
			})
			if t != nil {
				t.startTime = time.Now()
				t.status = ScheduledTask
				msg.response.TaskType = ReduceType
				msg.response.NReduce = c.nReduce
				msg.response.ImFiles = c.imFiles
				msg.response.ReduceKey = t.reduceKey
			} else {
				msg.response.TaskType = IdleType
			}
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			if msg.request.TaskType == ReduceType {
				t := c.allocTaskWithFunc(func(t *task) bool {
					return t.reduceKey == msg.request.ReduceKey
				})
				if t != nil && t.status == ScheduledTask {
					t.status = FinishedTask
				}
			}
			msg.ok <- struct{}{}
		}
	}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	select {
	case _, ok := <-c.doneReduce:
		ret = !ok
	default:
		//
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	os.MkdirAll("/var/tmp/6.824/mr", 0755)
	logfile, _ := os.Create("/var/tmp/6.824/mr/coordinator.log")
	log.SetOutput(logfile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	defer logfile.Close()

	c := Coordinator{}

	// Your code here.

	c.nReduce = nReduce
	c.files = append(c.files, files...)

	c.pollingCh = make(chan *pollingMsg)
	c.reportCh = make(chan *reportMsg)

	c.doneMapCh = make(chan struct{})
	c.doneReduce = make(chan struct{})

	go c.scheduleMap()
	go c.scheduleReduce()

	c.server()
	return &c
}
