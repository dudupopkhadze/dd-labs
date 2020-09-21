package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

//HandOutJobArg for calling master's handajob
type HandOutJobArg struct{ ID int }

//HandOutJobResponse response type from master's HandOutJob
type HandOutJobResponse struct {
	JobID    int
	Files    []string
	JobType  JobType
	MapID    int
	ReduceID int
	NReduce  int
}

//InitWorkerArgs args for master's initme
type InitWorkerArgs struct{}

// InitWorkerResponse response for master's InitWorker
// returns id for this worker
type InitWorkerResponse struct {
	ID int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
