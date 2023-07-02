package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
)

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	bucket  int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	// Cryptographic hash 32 bit FNV-1a hash Ex. 0x1400037b2a8
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
	}

	w.RequestMapTask()
	w.RequestBucketAssignment(reducef)

}
func HashPartitioner(kva []KeyValue, NumReduce int) [][]KeyValue {
	kvas := make([][]KeyValue, NumReduce)

	for _, kv := range kva {
		v := ihash(kv.Key) % NumReduce
		// for each keyvalue pair, it will hash the key

		// determines what reducer each keyvalue should be sent to
		// by modulo of the number of reducers specified
		// *essentially load balancing
		kvas[v] = append(kvas[v], kv)
		// adds the key value pair to assigned reducer
	}
	// returns a nested list containg all the key value
	// pairs assigned to their associated reduce assignment
	return kvas
}

func (w *WorkerSt) RequestMapTask() {
	for {
		args := IntermediateFile{}
		reply := MapTask{}
		call("Coordinator.RequestMapTask", &args, &reply)
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()

		kva := w.mapf(reply.Filename, string(content))
		// store kva in multiple files according to rules described in the README

		// for each map task, file is read and hash partitioned
		// this assigns the tasks (load balances) for the number of
		// reducers specified

		kvas := HashPartitioner(kva, reply.NumReduce)

		for i := 0; i < reply.NumReduce; i++ {
			// This will create *NumReduce* intermediate files for every
			// file *8*
			// In this case, 80 total files

			filename := WriteIntermediateFiles(kvas[i], reply.MapNumAssignment, i)
			// sends each intermediate filename to coordinator reduce task channel
			SendIntermediateFile(filename, i)
		}
		emptyReply := EmptyReply{}
		call("Coordinator.MapTaskCompleted", &reply, &emptyReply)

		if emptyReply.MapCompleted {
			break
		}
	}
}

func (w *WorkerSt) RequestBucketAssignment(reducef func(string, []string) string) {
	// will get a bucket number assigned from the coordinator, then find all intermediatew files in cache that share that bucket number
	args := IntermediateFile{}
	reply := Buckets{}

	for {
		assignedfiles := []string{}
		call("Coordinator.RequestBucketAssignment", &args, &reply)
		pattern := regexp.MustCompile(fmt.Sprintf("mr-.*-%d", reply.Bucket))
		dir, _ := os.Getwd()
		d, _ := os.Open(dir)

		defer d.Close()

		files, _ := d.Readdir(-1)
		for _, file := range files {

			if pattern.FindStringSubmatch(file.Name()) != nil {
				assignedfiles = append(assignedfiles, file.Name())
			}
		}
		// assigned files has all the intermediate files with associated bucket number
		w.RequestReduceTask(reducef, reply.Bucket, assignedfiles)
		emptyReply := EmptyReply{}
		//send the cooridnator bucket task finished
		//receive a break notification in return if the coordinator sayd all is done
		call("Coordinator.ReduceTaskCompleted", &reply, &emptyReply)

		if emptyReply.ReduceCompleted {
			break
		}
		// here we need to tell the coordinator we finished a bucket task
		// the coordinator will keep track of all bucket tasks
		// when all is complete, the coordinator will wait then Done()
	}
}

func (w *WorkerSt) RequestReduceTask(reducef func(string, []string) string, bucket int, filestoreduce []string) {

	intermediate := []KeyValue{}

	for i := 0; i < len(filestoreduce); i++ {
		// for every intermediate file task

		file, _ := os.Open(filestoreduce[i])

		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if dec.Decode(&kv) != nil {
				// breaks out of each intermediate file after all kv appended
				break
			}
			intermediate = append(intermediate, kv)
		}

	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(bucket)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-*BucketNumber* .
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

}

func SendIntermediateFile(filename string, bucketnum int) ReduceTask {
	// This function will get each intermediate filename
	// and send it to the Coordinator for it to add to
	// the reducer task channel
	args := InterFiles{}
	args.Filename = filename
	args.ReduceBucketNum = bucketnum

	reducereply := ReduceTask{}
	res := call("Coordinator.IntermediateFileHandler", &args, &reducereply)
	_ = res

	return reducereply
}

func WriteIntermediateFiles(intermediate []KeyValue, MapTaskNum, ReduceID int) string {

	filename := "mr-" + strconv.Itoa(MapTaskNum) + "-" + strconv.Itoa(ReduceID)
	// mr-(task number)-(reduce number)
	file, _ := os.Create(filename)
	// Creates the file with naming convention
	enc := json.NewEncoder(file)
	// encodes each keyvalue pair to the json file
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		_ = err
	}

	return filename
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
