package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)
import "hash/fnv"

type TaskRole int

const (
	Mapper  TaskRole = 0
	Reducer TaskRole = 1
)

type Task struct {
	TaskRole  TaskRole // map or reduce
	MapInput  string   // map Task's input file
	MapNum    int      // total map tasks' number
	ReduceNum int      // total reduce tasks' number
	TaskId    int      // the id of the map or reduce
	Done      bool     // current Task is done
}

type MyWorker struct {
	WorkerId int
	task     Task
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getMapFileName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%v-%v", mapId, reduceId)
}

func getReduceFileName(reduceId int) string {
	return fmt.Sprintf("mr-out-%v", reduceId)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	worker := MyWorker{mapf: mapf, reducef: reducef}
	worker.run()
}

func (w *MyWorker) run() {
	w.register()
	for {
		w.requestTask()
		if w.task.TaskRole == Mapper {
			w.mapper()
		} else {
			w.reducer()
		}
		w.reportTask()
	}
}

func (w *MyWorker) mapper() {
	readFile, err := ioutil.ReadFile(w.task.MapInput)
	if err != nil {
		return
	}
	kv := w.mapf(w.task.MapInput, string(readFile))
	reduceRes := make([][]KeyValue, w.task.ReduceNum)
	for _, value := range kv {
		idx := ihash(value.Key) % w.task.ReduceNum
		reduceRes[idx] = append(reduceRes[idx], value)
	}
	for i, kvs := range reduceRes {
		fileName := getMapFileName(w.task.TaskId, i)
		tempFile, err := ioutil.TempFile("", fileName)
		defer os.Remove(tempFile.Name())
		if err != nil {
			return
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				return
			}
		}
		os.Rename(tempFile.Name(), fileName)
	}
	w.task.Done = true
}
func (w *MyWorker) reducer() {
	//panic("haven't impl")
	kva := make(map[string][]string)
	var ks []string
	for i := 0; i < w.task.MapNum; i++ {
		mapFileName := getMapFileName(i, w.task.TaskId)
		readFile, err := os.Open(mapFileName)
		if err != nil {
			return
		}
		dec := json.NewDecoder(readFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := kva[kv.Key]; !ok {
				ks = append(ks, kv.Key)
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
	}
	sort.Strings(ks)
	fileName := getReduceFileName(w.task.TaskId)
	tempFile, err := ioutil.TempFile("", fileName)
	defer os.Remove(tempFile.Name())
	for _, key := range ks {
		res := w.reducef(key, kva[key])
		if err != nil {
			return
		}
		_, err = tempFile.WriteString(fmt.Sprintf("%v %v\n", key, res))
		if err != nil {
			return
		}
	}
	os.Rename(tempFile.Name(), fileName)
	w.task.Done = true
}

func (w *MyWorker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	if call("Coordinator.Register", &args, &reply) != true {
		os.Exit(1)
	}
	w.WorkerId = reply.WorkerId
}

func (w *MyWorker) requestTask() {
	args := RequestTaskArgs{w.WorkerId}
	reply := RequestTaskReply{}
	if call("Coordinator.RequestTask", &args, &reply) != true {
		os.Exit(1)
	}
	w.task = reply.Task
	//log.Println("got new Task %v", w.task)
}

func (w *MyWorker) reportTask() {
	args := ReportTaskArgs{w.WorkerId, w.task}
	reply := ReportTaskReply{}
	if call("Coordinator.ReportTask", &args, &reply) != true {
		os.Exit(1)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
