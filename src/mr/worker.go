package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//MapOverFile maps given func over given file's content
func MapOverFile(
	file string,
	mapFN func(string, string) []KeyValue,
) (currentKeyValue []KeyValue) {
	openedFile, err := os.Open(file)
	if err != nil {
		log.Fatalln(err)
	}
	defer openedFile.Close()

	r, err := ioutil.ReadAll(openedFile)
	if err != nil {
		log.Fatalln("error reading file for map ")
	}
	currentKeyValue = mapFN(file, string(r))
	return
}

//Map handles mapjob
func Map(
	ID int,
	mapFN func(string, string) []KeyValue,
	files []string, mapID int, NReduce int) (generatedFileNames []string) {
	keyValues := make([]KeyValue, 0)
	for _, file := range files {
		keyValues = append(keyValues, MapOverFile(file, mapFN)...)
	}
	generatedFileNames = make([]string, len(keyValues))
	for _, keyValue := range keyValues {
		generatedFileName := fmt.Sprintf("mr -%v-%v-%v", mapID, ihash(keyValue.Key)%NReduce, ID)
		r, err := os.OpenFile(generatedFileName,
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalln(err)
		}
		defer r.Close()
		generatedFileNames = append(generatedFileNames, generatedFileName)

		fmt.Fprintf(r, "%v %v\n", keyValue.Key, keyValue.Value)
	}
	return
}

// WorkUntilDeath wants to get job
func WorkUntilDeath(ID int) {
	for {

		givenJob := CallHandOutJob(ID)

		if !givenJob.JobType.IsValid() {
			log.Fatalln("unsuported job type")
		}

		switch givenJob.JobType {
		case mapJob:
			fmt.Print("map")
		case reduceJob:
			fmt.Print("red")

		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	ID := CallInit()
	if ID == -1 {
		log.Fatalln("pizdec")
	}
	WorkUntilDeath(ID)
}

//CallHandOutJob gets jobb from master if any job is loafting
func CallHandOutJob(ID int) (res HandOutJobResponse) {
	arg := HandOutJobArg{ID: ID}
	res = HandOutJobResponse{}

	success := call("Master.HandOutJob", &arg, &res)
	if !success {
		fmt.Printf("error handmejoob")
	}

	fmt.Printf("sucdsadsadcess  %v\n", res.JobID)

	return
}

//CallInit gets id from master through rpc
func CallInit() (ID int) {
	arg := RPCEmptyArgument{}
	res := InitWorkerResponse{}

	success := call("Master.InitWorker", &arg, &res)
	if !success {
		return -1
	}
	ID = res.ID
	return
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
