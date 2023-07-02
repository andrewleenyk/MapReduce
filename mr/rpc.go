package mr

import (
	"os"
	"strconv"
)

type EmptyArs struct {
}

type AllBuckets struct {
	ReduceFileList []string
}

type NewArs struct {
	BucketNumRequest int
	NoBucketsLeft    bool
}

type Buckets struct {
	Bucket int
}

type EmptyReply struct {
	MapCompleted    bool
	ReduceCompleted bool
}

type ReduceTask struct {
	Filename        string
	ReduceBucketNum int
}

type InterFiles struct {
	Filename        string
	ReduceBucketNum int
}

type IntermediateFile struct {
	MessageType   int
	MessageCount  string
	NumReduceType int
}

type MapTask struct {
	Filename         string
	NumReduce        int
	MapNumAssignment int
	TaskType         string
	ReduceFileList   []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
