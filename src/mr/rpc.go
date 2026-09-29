package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type TaskRequest struct {
}

type TaskReply struct {
	TaskType string
	//这个任务是“Map”，“Reduce”，“wait”，“exit”
	MapTaskFileName string
	MapID           int
	ReduceID        int
	MaxMapTaskID    int
	NReduce         int
}

type TaskDoneArgs struct {
	TaskType string
	TaskID   int
}

type TaskDoneReply struct {
}
