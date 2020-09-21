package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	ID := CallInit()
	if ID == -1 {
		log.Fatalln("pizdec")
	}

	givenJob := CallHandOutJob(ID)
	fmt.Printf("success  %v\n", givenJob.JobID)
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
	arg := InitWorkerArgs{}
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
