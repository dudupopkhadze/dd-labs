package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue Map functions return a slice of KeyValue.
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

func fillUpFileFromKeyValues(
	reducef func(string, []string) string,
	keyValues []KeyValue, file *os.File) {
	sort.Sort(IKeyValueArr(keyValues))
	i := 0
	for i < len(keyValues) {
		j := i + 1
		for j < len(keyValues) && keyValues[j].Key == keyValues[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, keyValues[k].Value)
		}
		fmt.Fprintf(file, "%v %v\n", keyValues[i].Key, reducef(keyValues[i].Key, values))
		i = j
	}
}

//Reduce handles given reduce job
func Reduce(
	ID int,
	reduceID int,
	files []string,
	reducef func(string, []string) string) (generatedFiles []string) {
	keyValues := make([]KeyValue, 0)
	for _, file := range files {
		openedFile := openFile(file)

		for {
			k := ""
			v := ""
			nRead, err := fmt.Fscanf(openedFile, "%s %s", &k, &v)
			if nRead != 2 || err != nil {
				break
			}
			keyValues = append(keyValues, KeyValue{Key: k, Value: v})
		}
		openedFile.Close()
	}

	name := fmt.Sprintf("mr-out-%v-%v", reduceID, ID)
	file := createFile(name)
	fillUpFileFromKeyValues(reducef, keyValues, file)
	file.Close()
	generatedFiles = []string{name}
	return

}

//mapOverFile maps given func over given file's content
func mapOverFile(
	file string,
	mapFN func(string, string) []KeyValue,
) (currentKeyValue []KeyValue) {
	openedFile := openFile(file)
	defer openedFile.Close()

	currentKeyValue = mapFN(file, readFileAsString(openedFile))
	return
}

func generateFileName(mapID int, workerID int, keyValue KeyValue, NReduce int) (generatedFileName string) {
	generatedFileName = fmt.Sprintf("mr-%v-%v-%v", mapID, ihash(keyValue.Key)%NReduce, workerID)
	r := openFileWithWrite(generatedFileName)
	fmt.Fprintf(r, "%v %v\n", keyValue.Key, keyValue.Value)
	r.Close()
	return
}

//Map handles mapjob
func Map(
	ID int,
	mapFN func(string, string) []KeyValue,
	files []string, mapID int, NReduce int) (generatedFileNames []string) {
	keyValues := make([]KeyValue, 0)
	for _, file := range files {
		keyValues = append(keyValues, mapOverFile(file, mapFN)...)
	}
	temp := make(map[string]bool)
	for _, keyValue := range keyValues {
		temp[generateFileName(mapID, ID, keyValue, NReduce)] = true
	}
	generatedFileNames = make([]string, 0)
	for a := range temp {
		generatedFileNames = append(generatedFileNames, a)
	}
	return
}

// workUntilDeath asks and execs job in loop
func workUntilDeath(
	ID int,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		givenJob := CallHandOutJob(ID)

		if !givenJob.JobType.IsValid() {
			log.Fatalln("Error unsuported job type")
		}

		switch givenJob.JobType {
		case mapJob:
			CallHandJobRunDown(givenJob.JobID, Map(ID, mapf, givenJob.Files, givenJob.MapID, givenJob.NReduce))
		case reduceJob:
			CallHandJobRunDown(givenJob.JobID, Reduce(ID, givenJob.ReduceID, givenJob.Files, reducef))
		default:
			time.Sleep(time.Second)
		}
	}
}

//Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	ID := CallInit()
	workUntilDeath(ID, mapf, reducef)
}

//CallHandJobRunDown call masters handjobrundwos method
func CallHandJobRunDown(ID int, GeneratedFiles []string) {
	arg := HandJobRunDownArgs{}
	arg.GeneratedFiles = GeneratedFiles
	arg.ID = ID
	res := RPCEmptyArgument{}
	success := call("Master.HandJobRunDown", &arg, &res)
	if !success {
		log.Fatalln("Error handjubrundown")
	}
}

//CallHandOutJob gets jobb from master if any job is loafting
func CallHandOutJob(ID int) (res HandOutJobResponse) {
	arg := RPCIDArgument{ID: ID}
	res = HandOutJobResponse{}

	success := call("Master.HandOutJob", &arg, &res)
	if !success {
		log.Fatalln("Error handmejoob")
	}
	return
}

//CallInit gets id from master through rpc
func CallInit() int {
	arg := RPCEmptyArgument{}
	res := RPCIDArgument{}

	success := call("Master.InitWorker", &arg, &res)
	if !success {
		log.Fatalln("Error calling initworker")
	}

	return res.ID
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
