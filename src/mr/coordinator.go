package mr

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	id      int
	file    []string
	startAt time.Time
	done    bool
}

type Coordinator struct {
	mutex        sync.Mutex
	mapTasks     []Task
	mapRemain    int
	reduceTasks  []Task
	reduceRemain int
}

func (c *Coordinator) GetTask(fn *TaskFinished, td *TaskToDo) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if fn.DoneType == TaskTypeMap && c.mapTasks[fn.ID].done == false {
		for i, j := range fn.Files {
			if len(j) > 0 {
				c.reduceTasks[i].file = append(c.reduceTasks[i].file, fn.Files[0])
				//= append(c.reduceTasks[i].file, fn.Files[0])
			}
		}
		c.mapTasks[fn.ID].done = true
		c.mapRemain--
	} else if fn.DoneType == TaskTypeReduce && c.reduceTasks[fn.ID].done == false {
		c.reduceTasks[fn.ID].done = true
		c.reduceRemain--
	} else {
		log.Warning("ignore bad task type")
	}

	//now := time.Now()
	outTime := time.Now().Add(-10 * time.Second)
	if c.mapRemain > 0 {
		for i, t := range c.mapTasks {
			if t.done == false && t.startAt.Before(outTime) {
				td.Type = TaskTypeMap
				td.ID = t.id
				td.Files = t.file
				td.NReduce = len(c.reduceTasks)
				c.reduceTasks[i].startAt = time.Now()
				return nil
			}
		}
	} else if c.reduceRemain > 0 {
		for i, t := range c.reduceTasks {
			if t.done == false && t.startAt.Before(outTime) {
				td.Type = TaskTypeReduce
				td.ID = t.id
				td.Files = t.file
				c.reduceTasks[i].startAt = time.Now()
				return nil
			}
		}
		td.Type = TaskTypeSleep
	} else {
		td.Type = TaskTypeExit
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.mapRemain+c.reduceRemain == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapRemain = len(files)
	c.reduceRemain = nReduce
	c.mapTasks = []Task{}
	c.reduceTasks = []Task{}
	for i := range files {
		c.mapTasks = append(c.mapTasks, Task{
			id:      i,
			file:    []string{files[i]},
			startAt: time.Time{},
			done:    false,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			id:      i,
			file:    nil,
			startAt: time.Time{},
			done:    false,
		})
	}

	c.server()
	return &c
}
