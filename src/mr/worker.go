package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	workerId := getWordId()

	for true {

		args := TaskCallArgs{WorkerId: workerId}
		reply := TaskCallReply{}

		err := rpcCall("Coordinator.HandleTaskCall", &args, &reply)
		if err == nil {
			task := reply.Task
			if task.CState == MAP {
				doMap(workerId, task, mapf)
			} else if task.CState == REDUCE {
				doReduce(workerId, task, reducef)
			} else if task.CState == WAIT {
				time.Sleep(time.Second * 3)
			} else if task.CState == EXIT {
				break
			}
		} else {
			break
		}

	}

}

// 先输出到临时文件再重命名
func doMap(workerId string, task task, mapf func(string, string) []KeyValue) {
	tmpFiles := make([]*os.File, task.NReduce)
	dirPath, _ := os.Getwd()
	// 创建临时文件
	for i := 0; i < task.NReduce; i++ {
		tmpName := fmt.Sprintf("mr-tmp-%d-%d", task.TaskID, i)
		f, err := os.CreateTemp(dirPath, tmpName)
		defer f.Close()
		if err != nil {
			log.Fatalf("create temp file error: %s\n", err.Error())
			return
		}
		tmpFiles[i] = f
	}
	// 读取文件内容并写入临时文件
	content := readFileFromLocal(task.MFile)
	keyValues := mapf(task.MFile, content)
	for _, kv := range keyValues {
		idx := ihash(kv.Key) % task.NReduce
		f := tmpFiles[idx]
		encoder := json.NewEncoder(f)
		err := encoder.Encode(kv)
		if err != nil {
			log.Fatalf("encode json error: %s\n", err.Error())
			return
		}
	}
	// 重命名临时文件
	tFilePaths := make([]string, 0)
	for i, tmpFile := range tmpFiles {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		onamePath := fmt.Sprintf("%s/%s", dirPath, oname)
		err := os.Rename(tmpFile.Name(), onamePath)
		if err != nil {
			log.Fatalf("rename file error: %s\n", err.Error())
			return
		}
		tFilePaths = append(tFilePaths, onamePath)
	}

	rpcCall("Coordinator.HandleMapComplCall", &MapComplCallArgs{TempFiles: tFilePaths, WorkerId: workerId}, &MapComplCallReply{})
}

func doReduce(workerId string, task task, reducef func(string, []string) string) {
	files := task.RFiles
	allKvs := make([]KeyValue, 0)
	for _, file := range files {
		kvs := readKvsFromLocal(file)
		allKvs = append(allKvs, kvs...)
	}
	sort.Sort(ByKey(allKvs))
	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	f, err := os.Create(oname)
	defer f.Close()
	if err != nil {
		log.Fatalf("cannot create %v\n", oname)
		return
	}
	for i := 0; i < len(allKvs); {
		j := i + 1
		for j < len(allKvs) && allKvs[i].Key == allKvs[j].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, allKvs[k].Value)
		}
		output := reducef(allKvs[i].Key, values)
		fmt.Fprintf(f, "%v %v\n", allKvs[i].Key, output)
		i = j
	}

	rpcCall("Coordinator.HandlerReduceComplCall", &ReduceComplCallArgs{WorkerId: workerId}, &ReduceComplCallReply{})
}

func rpcCall(rpcname string, args interface{}, reply interface{}) error {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Call(rpcname, args, reply)
}

func readFileFromLocal(filename string) string {
	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		log.Fatalf("cannot open %v\n", filename)
	}
	content, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v\n", filename)
	}
	return string(content)
}

func readKvsFromLocal(filename string) (kvs []KeyValue) {
	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		log.Fatalf("cannot open %v\n", filename)
	}
	dec := json.NewDecoder(f)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func getWordId() string {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf(err.Error())
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
