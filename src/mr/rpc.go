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

//RPCEmptyArgument args
type RPCEmptyArgument struct{}

//RPCIDArgument for calling methods with int id argument
type RPCIDArgument struct{ ID int }

//HandOutJobResponse response type from master's HandOutJob
type HandOutJobResponse struct {
	JobID    int
	Files    []string
	JobType  JobType
	MapID    int
	ReduceID int
	NReduce  int
}

//HandJobRunDownArgs for maters handjobrundown
type HandJobRunDownArgs struct {
	ID             int
	GeneratedFiles []string
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
