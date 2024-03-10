package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type cState int

const (
	MAP cState = iota
	REDUCE
	WAIT
	EXIT
)

type task struct {
	CState  cState
	TaskID  int
	NReduce int
	MFile   string
	RFiles  []string
}

// Add your RPC definitions here.

type TaskCallArgs struct {
	WorkerId string
}

type TaskCallReply struct {
	Task task
}

type MapComplCallArgs struct {
	WorkerId  string
	TempFiles []string
}

type MapComplCallReply struct {
}

type ReduceComplCallArgs struct {
	WorkerId string
}

type ReduceComplCallReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
