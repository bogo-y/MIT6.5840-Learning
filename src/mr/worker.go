package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Kvs []KeyValue

func (c Kvs) Len() int {
	return len(c)
}
func (c Kvs) Less(i int, j int) bool {
	return c[i].Key < c[j].Key
}
func (c Kvs) Swap(i int, j int) {
	c[i], c[j] = c[j], c[i]
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	finished := TaskFinished{}
	for {
		todo, err1 := getToDo(&finished)
		cnt := 1
		for err1 != nil && cnt <= 3 {
			log.Error(err1.Error() + " retry after 200ms")
			//	fmt.Println(err1.Error() + " retry after 100ms")
			time.Sleep(time.Microsecond * 200)
			todo, err1 = getToDo(&finished)
			cnt += 1
		}
		if err1 != nil {
			log.Fatal(err1.Error() + " no retry remaining")
			//fmt.Println(err1)
			//fmt.Println("no retry remaining")
			return
		}
		switch todo.Type {
		case TaskTypeExit:
			log.Info("get exit task")
			return
		case TaskTypeSleep:
			time.Sleep(time.Duration(todo.ID) * time.Millisecond)
		case TaskTypeMap:
			file, err2 := os.Open(todo.Files[0])
			if err2 != nil {
				log.Error("cant open " + todo.Files[0] + " " + err2.Error())
				continue
			}
			content, err3 := io.ReadAll(file)
			err := file.Close()
			if err != nil {
				log.Error(err)
			}
			if err3 != nil {
				log.Error("cant read " + todo.Files[0] + " " + err3.Error())
				continue
			}
			intermediate := mapf(todo.Files[0], string(content))
			shuffleFiles := make(map[int][]KeyValue)
			for _, kv := range intermediate {
				idx := ihash(kv.Key) % todo.NReduce
				shuffleFiles[idx] = append(shuffleFiles[idx], kv)
			}
			files := make([]string, todo.NReduce)
			for rid, kvs := range shuffleFiles {
				fileName := fmt.Sprintf("mr-%d-%d", todo.ID, rid)
				interFile, _ := os.Create(fileName)
				enc := json.NewEncoder(interFile)
				for i := range kvs {
					err = enc.Encode(&kvs[i])
					if err != nil {
						log.Fatal(err)
					}
				}
				err = interFile.Close()
				if err != nil {
					log.Error(err)
				}
				files[rid] = fileName
			}
			finished = TaskFinished{
				DoneType: TaskTypeMap,
				ID:       todo.ID,
				Files:    files,
			}
		case TaskTypeReduce:
			opName := "mr-out-" + strconv.Itoa(todo.ID)
			output, err4 := os.Create(opName)
			if err4 != nil {
				log.Error(err4)
				continue
			}
			intermediate := []KeyValue{}
			for i := range todo.Files {
				file, err1 := os.Open(todo.Files[i])
				if err1 != nil {
					log.Error("cannot open " + todo.Files[0] + " " + err1.Error())
					continue
				}

				decoder := json.NewDecoder(file)
				var kv KeyValue
				err2 := decoder.Decode(&kv)
				for err2 == nil {
					intermediate = append(intermediate, kv)
					err2 = decoder.Decode(&kv)
				}
				err3 := file.Close()
				if err3 != nil {
					log.Error(err3)
				}
			}
			sort.Sort(Kvs(intermediate))
			for i := 0; i < len(intermediate); {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				_, err5 := fmt.Fprintf(output, "%v %v\n", intermediate[i].Key, reducef(intermediate[i].Key, values))
				if err5 != nil {
					log.Error(err5)
				}
				i = j
			}
			finished = TaskFinished{
				DoneType: TaskTypeReduce,
				ID:       todo.ID,
				Files:    []string{opName},
			}
		case TaskTypeNone:
			log.Info("received task type none")
		default:
			log.Error("bad task type")
		}
	}

}

func getToDo(finished *TaskFinished) (*TaskToDo, error) {
	todo := TaskToDo{}
	ok := call("Coordinator.GetTask", finished, &todo)
	if ok == false {
		//fmt.Println("Failed to call")
		//os.Exit(0)
		return nil, errors.New("failed to call coordinator")
	}
	return &todo, nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
