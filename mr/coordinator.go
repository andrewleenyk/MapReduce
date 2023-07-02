package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	Files                []string
	FilesToReduce        []string
	ReduceNum            int
	MapTasks             chan MapTask
	ReduceTasks          chan ReduceTask
	BucketNumbers        chan Buckets
	MapTaskCountID       int
	FinishedMapTasks     map[string]bool
	FinishedReduceTasks  map[string]bool
	IntermediateFilename [][]string

	ReducedFinished bool

	Lock sync.Mutex
}

// CallHandler distributes the unfinished tasks to the channel
func (c *Coordinator) CallHandler() {
	fmt.Println("Distributing unfinished tasks to the channel")
	// iterates over files and adds them to the channel as mapTask structs
	filenum := 0
	for _, file := range c.Files {
		mapTask := MapTask{
			Filename:         file,
			NumReduce:        c.ReduceNum,
			MapNumAssignment: filenum,
		}

		fmt.Println("MapTask", mapTask, "added to channel")
		c.MapTasks <- mapTask
		c.FinishedMapTasks["map_"+mapTask.Filename] = false
		filenum++
	}
	for i := 0; i < c.ReduceNum; i++ {
		c.FinishedReduceTasks["reduce_"+strconv.Itoa(i)] = false
		Bucket := Buckets{
			Bucket: i,
		}
		c.BucketNumbers <- Bucket
	}
	// adds buckets to channel
	// each worker requests a bucketnumber
	// each worker requests a task with that bucketnumber
	// if nil, then the worker requests a new bucketnumber

}

// intermediate files handler
func (c *Coordinator) IntermediateFileHandler(args *InterFiles, reply *ReduceTask) error {
	filename := args.Filename
	bucketnum := args.ReduceBucketNum

	reduceTask := ReduceTask{
		Filename:        filename,
		ReduceBucketNum: bucketnum,
	}
	c.ReduceTasks <- reduceTask

	return nil
}

// assigns the worker to a bucket number for which it uses to reduce intermediate files
func (c *Coordinator) RequestBucketAssignment(args *EmptyArs, reply *Buckets) error {
	assigned, _ := <-c.BucketNumbers
	*reply = assigned

	return nil
}

// gives worker a map task
func (c *Coordinator) RequestMapTask(args *EmptyArs, reply *MapTask) error {
	// checks to see if task exists
	task, _ := <-c.MapTasks
	fmt.Println("Map task found,", task.Filename)
	*reply = task

	go c.WaitForWorker(task)

	return nil
}

func (c *Coordinator) WaitForWorker(task MapTask) {
	// waits 3 seconds to check the worker again on completion
	time.Sleep(time.Second * 3)
	c.Lock.Lock()
	if c.FinishedMapTasks["map_"+task.Filename] == false {
		fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}

// reports if map task is complete
func (c *Coordinator) MapTaskCompleted(args *MapTask, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.FinishedMapTasks["map_"+args.Filename] = true
	if len(c.MapTasks) == 0 {
		time.Sleep(time.Second * 5)

		reply.MapCompleted = true
	}
	return nil
}

// reports if reduce task is complete
func (c *Coordinator) ReduceTaskCompleted(args *Buckets, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.FinishedReduceTasks["reduce_"+strconv.Itoa(args.Bucket)] = true

	if len(c.BucketNumbers) == 0 {
		time.Sleep(time.Second * 5)
		reply.ReduceCompleted = true
		c.ReducedFinished = true
	}
	c.Done()
	return nil
}

// starts a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.

func (c *Coordinator) Done() bool {
	ret := false

	if c.ReducedFinished {
		time.Sleep(1 * time.Second)
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:               files,
		FilesToReduce:       make([]string, 0),
		ReduceNum:           nReduce,
		MapTaskCountID:      0,
		MapTasks:            make(chan MapTask, 100),
		ReduceTasks:         make(chan ReduceTask, 100),
		BucketNumbers:       make(chan Buckets, nReduce),
		FinishedMapTasks:    make(map[string]bool),
		FinishedReduceTasks: make(map[string]bool),
		ReducedFinished:     false,
	}
	fmt.Println(files)

	c.server()
	c.CallHandler()
	return &c
}
