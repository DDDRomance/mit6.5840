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

type Coordinator struct {
	// Your definitions here.
	MaxMapTaskID int
	nReduce      int

	files             []string
	MapTaskStatus     []bool
	MapTaskDoneStatus []bool

	ReduceTaskStatus     []bool
	ReduceTaskDoneStatus []bool

	mu sync.Mutex

	Status string

	DoneMapTaskTotal    int
	DoneReduceTaskTotal int

	DoneStatus bool
	// 判断现在是"Map"阶段还是“Reduce”阶段

	MapTaskStartTime    []time.Time
	ReduceTaskStartTime []time.Time
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "Map" {
		if c.MapTaskDoneStatus[args.TaskID] == false {
			c.MapTaskDoneStatus[args.TaskID] = true
			c.DoneMapTaskTotal++
		}

		if c.DoneMapTaskTotal == c.MaxMapTaskID {
			c.Status = "Reduce"
		}
	} else {
		if args.TaskType == "Reduce" && c.ReduceTaskDoneStatus[args.TaskID] == false {
			c.ReduceTaskDoneStatus[args.TaskID] = true
			c.DoneReduceTaskTotal++
		}

		if c.DoneReduceTaskTotal == c.nReduce {
			c.DoneStatus = true
		}
	}
	return nil
}

func (c *Coordinator) CheckTimeOut() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Status == "Map" {
		for i := 0; i < c.MaxMapTaskID; i++ {
			if c.MapTaskStatus[i] == true && c.MapTaskDoneStatus[i] == false {
				if time.Since(c.MapTaskStartTime[i]) >= 10*time.Second {
					c.MapTaskStatus[i] = false
				}
			}
		}
	} else if c.Status == "Reduce" {
		for i := 0; i < c.nReduce; i++ {
			if c.ReduceTaskStatus[i] == true && c.ReduceTaskDoneStatus[i] == false {
				if time.Since(c.ReduceTaskStartTime[i]) >= 10*time.Second {
					c.ReduceTaskStatus[i] = false
				}
			}
		}
	}

	return nil
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskReply) error {
	c.CheckTimeOut()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Status == "Map" {
		findTask := false
		for i := 0; i < c.MaxMapTaskID; i++ {
			if c.MapTaskStatus[i] == false {
				reply.TaskType = "Map"
				reply.MapTaskFileName = c.files[i]
				reply.MapID = i
				reply.MaxMapTaskID = c.MaxMapTaskID
				reply.NReduce = c.nReduce

				c.MapTaskStatus[i] = true
				c.MapTaskStartTime[i] = time.Now()
				findTask = true
				break
			}
		}

		if findTask == false {
			reply.TaskType = "Wait"
		}
	} else {
		if c.Status == "Reduce" {
			findTask := false
			for i := 0; i < c.nReduce; i++ {
				if c.ReduceTaskStatus[i] == false {
					reply.TaskType = "Reduce"
					reply.ReduceID = i
					reply.MaxMapTaskID = c.MaxMapTaskID

					c.ReduceTaskStatus[i] = true
					c.ReduceTaskStartTime[i] = time.Now()
					findTask = true
					break
				}
			}

			if findTask == false {
				if c.DoneStatus == true {
					reply.TaskType = "Exit"
				} else {
					reply.TaskType = "Wait"
				}
			}
		} else {
			log.Fatalf("Illegal Task Request Type\n")
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.DoneStatus
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// MaxTaskID = len(string)
	// Your code here.
	c.MaxMapTaskID = len(files)
	c.nReduce = nReduce
	c.Status = "Map"

	c.MapTaskStatus = make([]bool, len(files))
	c.MapTaskDoneStatus = make([]bool, len(files))

	c.ReduceTaskStatus = make([]bool, nReduce)
	c.ReduceTaskDoneStatus = make([]bool, nReduce)

	c.files = files
	c.DoneMapTaskTotal = 0
	c.DoneReduceTaskTotal = 0

	c.MapTaskStartTime = make([]time.Time, len(files))
	c.ReduceTaskStartTime = make([]time.Time, nReduce)

	c.server(sockname)
	return &c
}
