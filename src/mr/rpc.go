package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	TaskTypeNone = iota
	TaskTypeMap
	TaskTypeReduce
	TaskTypeSleep
	TaskTypeExit
)

type TaskFinished struct {
	DoneType int
	ID       int
	Files    []string
}
type TaskToDo struct {
	Type    int
	ID      int
	Files   []string
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
