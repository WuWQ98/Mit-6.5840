package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	cState  cState
	ch      chan task
	nReduce int
	mu      sync.Mutex
	mFiles  []fInfo
	rFiles  []fInfo
	workers map[string]wInfo
	wg      sync.WaitGroup
}

type wInfo struct {
	task      task
	startTime int64
	completed bool
}

type fInfo struct {
	filename  string
	filenames []string
	completed bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleTaskCall(args *TaskCallArgs, reply *TaskCallReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ch) > 0 {
		reply.Task = <-c.ch
		c.workers[args.WorkerId] = wInfo{
			task:      reply.Task,
			startTime: time.Now().Unix(),
			completed: false,
		}
	} else if c.cState == EXIT {
		reply.Task = task{CState: EXIT}
	} else {
		reply.Task = task{CState: WAIT}
	}
	return nil
}

// 接收map worker生成的中间文件位置
func (c *Coordinator) HandleMapComplCall(args *MapComplCallArgs, reply *MapComplCallReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerId := args.WorkerId
	tempFiles := args.TempFiles

	wInfo := c.workers[workerId]
	defer func() {
		wInfo.completed = true
		c.workers[workerId] = wInfo
	}()

	if c.mFiles[wInfo.task.TaskID].completed == true {
		return nil
	}

	for _, tempFile := range tempFiles {
		idx, _ := strconv.Atoi(tempFile[strings.LastIndex(tempFile, "-")+1:])
		c.rFiles[idx].filenames = append(c.rFiles[idx].filenames, tempFile)
	}
	c.mFiles[wInfo.task.TaskID].completed = true

	c.wg.Done()

	return nil
}

func (c *Coordinator) HandlerReduceComplCall(args *ReduceComplCallArgs, reply *ReduceComplCallReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerId := args.WorkerId

	wInfo := c.workers[workerId]
	defer func() {
		wInfo.completed = true
		c.workers[workerId] = wInfo
	}()

	if c.rFiles[wInfo.task.TaskID].completed == true {
		return nil
	}
	c.rFiles[wInfo.task.TaskID].completed = true
	c.wg.Done()
	return nil
}

func (c *Coordinator) checkTimeout() {
	for c.cState != EXIT {
		c.mu.Lock()
		for idx, wInfo := range c.workers {
			if now := time.Now().Unix(); now-wInfo.startTime > 10 && !wInfo.completed {
				wInfo.completed = true
				c.workers[idx] = wInfo
				c.ch <- wInfo.task
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) changeState() {
	c.wg.Wait()
	c.cState = REDUCE
	c.afterChangeToReduce()
	c.wg.Wait()
	c.cState = EXIT
}

func (c *Coordinator) afterChangeToReduce() {
	c.wg.Add(c.nReduce)
	for i, fInfo := range c.rFiles {
		c.ch <- task{
			CState: REDUCE,
			TaskID: i,
			RFiles: fInfo.filenames,
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	// 注册rpc对象
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
	return c.cState == EXIT
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(filenames []string, nReduce int) *Coordinator {

	c := Coordinator{
		cState:  MAP,
		ch:      make(chan task, max(nReduce, len(filenames))),
		nReduce: nReduce,
		mFiles:  make([]fInfo, len(filenames)),
		rFiles:  make([]fInfo, nReduce),
		workers: make(map[string]wInfo),
	}

	// Your code here.
	for i, filename := range filenames {
		c.ch <- task{
			CState:  MAP,
			TaskID:  i,
			NReduce: nReduce,
			MFile:   filename,
		}
		c.mFiles[i] = fInfo{
			filename:  filename,
			completed: false,
		}
	}
	c.wg.Add(len(filenames))

	c.server()

	go c.checkTimeout()

	go c.changeState()

	return &c
}
