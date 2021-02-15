package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterArgs struct {
	Current_id int64
}

type RegisterReply struct {
	Worker_id int64
}

type RequestTaskArgs struct {
	Worker_id int64
}

type RequestTaskReply struct {
	Task_type       TaskType 
	Task_id         int64
	Reduce_num      int64   //used only in map tasks, to slice bucket
	Input_file_name string  //used only in map tasks, the input file name
	Map_task_ids    []int64 //used in reduce task, to find intermediate files
}

type ReportMapTaskDoneArgs struct {
	Worker_id       int64
	Input_file_name string
	Task_id         int64
}

type ReportMapTaskDoneReply struct {
}

type ReportReduceTaskDoneArgs struct {
	Worker_id int64
	Task_id int64
}
type ReportReduceTaskDoneReply struct {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
