package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//singleJobSecondsLimit if workred work longer thank singleJobSecondsLimit second on a job - job is reasigned
const singleJobSecondsLimit = 10

//noWorker assigned ass job's workerId when jobs has no worker
const noWorker = -1

//noReduceJobID assigned to job.reduceId when it has no reduce job yet
const noReduceJobID = -1

//noMapJobID assigned to job.mapid when it has no map job yet
const noMapJobID = -1

//JobType  what job worker has
type JobType string

const (
	mapJob    JobType = "mapJob"
	reduceJob         = "reduceJob"
	noJob             = "noJob"
)

//IsValid checks if given jobtype has valid string value
func (jt JobType) IsValid() bool {
	switch jt {
	case mapJob, reduceJob, noJob:
		return true
	}
	return false
}

//JobState represents in which state given job is
type JobState string

const (
	active   JobState = "active"
	done              = "done"
	loafting          = "loafting"
)

//IsValid checks if given jobstate has valid string value
func (js JobState) IsValid() bool {
	switch js {
	case active, done, loafting:
		return true
	}
	return false
}

//Job struct
type Job struct {
	ID        int
	workerID  int
	files     []string
	jobState  JobState
	timestamp time.Time
	JobType   JobType
	mapID     int
	reduceID  int
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
	jlock      sync.Mutex // for locking jobs while handing out jobs
}

func fileNameSplitFn(rn rune) bool {
	return rn == '-'
}

func gentNewFileName(isMap bool, currentFileName string) string {
	splitedCurrentName := strings.FieldsFunc(currentFileName, fileNameSplitFn)
	if isMap {
		return fmt.Sprintf("mr-%v-%v", splitedCurrentName[1], splitedCurrentName[2])
	}

	return fmt.Sprintf("mr-out-%v", splitedCurrentName[2])

}

func getReduceIDFromFileName(currentFileName string) (ID int) {
	splitedCurrentName := strings.FieldsFunc(currentFileName, fileNameSplitFn)
	ID, err := strconv.Atoi(splitedCurrentName[2])
	if err != nil {
		log.Fatalln("Error parsing file name for getReduceId")
	}
	return
}

func shouldReasignJob(jobStartTime time.Time) bool {
	return time.Now().Sub(jobStartTime) >= singleJobSecondsLimit*time.Second
}

//handleMapRunDown handle job result after map
func (m *Master) handleMapRunDown(generatedFiles []string) {
	m.mlock.Lock()
	defer m.mlock.Unlock()
	m.fMaps++
	for _, file := range generatedFiles {
		name := gentNewFileName(true, file)
		renameFile(file, name)
		m.tempFiles = append(m.tempFiles, name)
	}
}

//hძზandleReduceRunDown handle job result after reduce
func (m *Master) hძზandleReduceRunDown(generatedFiles []string) {
	m.rlock.Lock()
	defer m.rlock.Unlock()
	m.fReduces++
	oldName := generatedFiles[0]
	renameFile(oldName, gentNewFileName(false, oldName))
}

///for rpc communications

//HandJobRunDown worker calls this method when it finishes given job
func (m *Master) HandJobRunDown(
	arg *HandJobRunDownArgs,
	res *RPCEmptyArgument) error {

	m.jlock.Lock()
	defer m.jlock.Unlock()

	job, ok := m.jobs[arg.ID]
	if !ok {
		log.Fatalf("Invalid task id from worker")
	}

	if !job.JobType.IsValid() {
		log.Fatalln("Unsuported job type")
	}

	if job.jobState == done {
		return nil //someone  finished this jobs already
	}
	job.jobState = done
	m.jobs[job.ID] = job

	switch job.JobType {
	case mapJob:
		m.handleMapRunDown(arg.GeneratedFiles)
	case reduceJob:
		m.hძზandleReduceRunDown(arg.GeneratedFiles)
	default:
		return nil
	}

	return nil
}

//HandOutJob hands out idle job to worker if it exists
func (m *Master) HandOutJob(
	arg *RPCIDArgument,
	res *HandOutJobResponse) error {
	res.JobType = noJob
	m.jlock.Lock()

	for _, job := range m.jobs {
		if job.jobState == loafting {
			job.timestamp = time.Now()
			res.Files = job.files
			res.JobID = job.ID
			res.NReduce = m.nReduce
			res.MapID = job.mapID
			res.JobType = job.JobType
			res.ReduceID = job.reduceID

			job.jobState = active
			job.workerID = arg.ID
			m.jobs[job.ID] = job

			break
		}
	}

	//unlock when done using jobs
	m.jlock.Unlock()

	return nil
}

//InitWorker tells worker it's id
func (m *Master) InitWorker(
	args *RPCEmptyArgument,
	reply *RPCIDArgument) error {
	reply.ID = m.nextWorker
	m.nextWorker++
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
func (m *Master) Done() bool {
	m.rlock.Lock()
	defer m.rlock.Unlock()

	return m.fReduces == m.nReduce
}

func (m *Master) isMapFinished() bool {
	m.mlock.Lock()
	defer m.mlock.Unlock()

	return m.fMaps == len(m.files)
}

//monitorJobsDuration monitorJobsDuration
func (m *Master) monitorJobsDuration() {
	for {
		//get latests jobs
		m.jlock.Lock()

		for i := range m.jobs {
			job := m.jobs[i]
			if job.jobState == active &&
				shouldReasignJob(job.timestamp) {
				job.jobState = loafting
				m.jobs[i] = job
			}
		}
		m.jlock.Unlock()
		time.Sleep(time.Second)
	}
}

//getFilesForReduce getFilesForReduce
func (m *Master) getFilesForReduce() {
	for {
		if m.isMapFinished() {
			break
		}
		time.Sleep(time.Second)
	}

	reduceFiles := make(map[int][]string)
	for _, filename := range m.tempFiles {
		ID := getReduceIDFromFileName(filename)
		reduceFiles[ID] = append(reduceFiles[ID], filename)
	}

	m.initReduceJobs(reduceFiles)
}

func (m *Master) initMapJobs() {
	for _, file := range m.files {
		newJobID := m.nextJob
		m.nextJob++
		newJob := Job{
			ID:        newJobID,
			JobType:   mapJob,
			files:     []string{file},
			jobState:  loafting,
			workerID:  noWorker,
			timestamp: time.Time{},
			mapID:     newJobID,
			reduceID:  noReduceJobID,
		}
		m.jobs[newJobID] = newJob
	}
}

func (m *Master) initReduceJobs(reduceFiles map[int][]string) {
	m.jlock.Lock()
	defer m.jlock.Unlock()

	for newReduceID := 0; newReduceID < m.nReduce; newReduceID++ {
		newJobID := m.nextJob
		m.nextJob++
		newJob := Job{
			ID:        newJobID,
			JobType:   reduceJob,
			files:     reduceFiles[newReduceID],
			jobState:  loafting,
			workerID:  noWorker,
			timestamp: time.Time{},
			mapID:     noMapJobID,
			reduceID:  newReduceID,
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
		jlock:      sync.Mutex{},
		nReduce:    nReduce,
		files:      files,
	}

	go m.initMapJobs()
	go m.monitorJobsDuration()
	go m.getFilesForReduce()

	m.server()
	return &m
}
