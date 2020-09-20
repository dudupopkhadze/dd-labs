package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

//JobType  what job worker has
type JobType string

const (
	mapJob    JobType = "mapJob"
	reduceJob         = "reduceJob"
	noJob             = "noJob"
)

//isValid checks if given jobtype has valid string value
func (jt JobType) IsValid() error {
	switch jt {
	case mapJob, reduceJob, noJob:
		return nil
	}
	return errors.New("Invalid Job type")
}

//JobState represents in which state given job is
type JobState string

const (
	active   JobState = "active"
	done              = "done"
	loafting          = "loafting"
)

//isValid checks if given jobtype has valid string value
func (js JobState) IsValid() error {
	switch js {
	case active, done, loafting:
		return nil
	}
	return errors.New("Invalid Job state")
}

type Master struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

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

	m.server()
	return &m
}
