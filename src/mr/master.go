package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

//noWorker assigned ass job's workerId when jobs has no worker
const noWorker = -1

//noReduceJobID assigned to job.reduceId when it has no reduce job yet
const noReduceJobID = -1

//JobType  what job worker has
type JobType string

const (
	mapJob    JobType = "mapJob"
	reduceJob         = "reduceJob"
	noJob             = "noJob"
)

//IsValid checks if given jobtype has valid string value
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

//IsValid checks if given jobstate has valid string value
func (js JobState) IsValid() error {
	switch js {
	case active, done, loafting:
		return nil
	}
	return errors.New("Invalid Job state")
}

//Job struct
type Job struct {
	jobID    int
	workerID int
	files    []string
	jobState JobState
	JobType  JobType
	mapID    int
	reduceID int
}

//Master struct
type Master struct {
	nReduce    int
	files      []string
	jobs       map[int]Job
	tempFiles  []string
	nextWorker int        //next free worker id
	nextJob    int        // job id
	fMaps      int        //dinished maps
	fReduces   int        // finished reduces
	mlock      sync.Mutex // for ensuring fMaps valide value
	rlock      sync.Mutex // for ensuring fReduces valide value
}

// Your code here -- RPC handlers for the worker to call.

///for rpc communications

//InitWorker tells worker it's id
func (m *Master) InitWorker(
	args *InitWorkerArgs,
	reply *InitWorkerResponse) error {
	reply.ID = m.nextWorker
	m.nextWorker++
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 2
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

//Done main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (m *Master) initJobs() {
	for _, file := range m.files {
		newJobID := m.nextJob
		m.nextJob++
		newJob := Job{
			jobID:    newJobID,
			JobType:  mapJob,
			files:    []string{file},
			jobState: loafting,
			workerID: noWorker,
		}
		m.jobs[newJobID] = newJob

	}
}

//MakeMaster // create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		tempFiles:  make([]string, 0),
		nextJob:    0,
		nextWorker: 0,
		jobs:       make(map[int]Job),
		fMaps:      0,
		fReduces:   0,
		mlock:      sync.Mutex{},
		rlock:      sync.Mutex{},
		nReduce:    nReduce,
		files:      files,
	}

	m.server()
	return &m
}
