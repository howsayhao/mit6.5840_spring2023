package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"io/ioutil"
	"strings"
	"path/filepath"
)

var mutex sync.Mutex


type Coordinator struct {
	// Your definitions here.
	mapTask map[string]bool
	mapAssign map[string]int
	reduceTask map[string]bool
	reduceAssign map[string]int
	nReduce int
	register map[int]time.Time
	backup map[string]bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleRequest(args *MapArgs, reply *MapReply) error {
	mutex.Lock()
	fmt.Printf("process %v requesting for map task\n", args.Worker)
	for mapfile, done := range c.mapTask {
		if !done && c.mapAssign[mapfile] == -1 {
			reply.Mf = mapfile
			reply.NReduce = c.nReduce
			c.mapAssign[mapfile] = args.Worker
			c.register[args.Worker] = time.Now()
			mutex.Unlock()
			return nil
		}	
	}
	for mapfile, backup := range c.backup {
		if backup && c.mapAssign[mapfile] != -1 {
			reply.Mf = mapfile
			reply.NReduce = c.nReduce
			c.register[args.Worker] = time.Now()
			mutex.Unlock()
			return nil
		}
	}
	mutex.Unlock()
	// fmt.Printf("no more idle map task\n")
	return nil
}

func (c *Coordinator) HandleReduce(args *ReduceArgs, reply *ReduceReply) error {
	mutex.Lock()
	for reducer, done := range c.reduceTask {
		if !done && c.reduceAssign[reducer] == -1 {
			reply.Reducer = reducer
			c.reduceAssign[reducer] = args.Worker
			c.register[args.Worker] = time.Now()
			mutex.Unlock()
			return nil
		}	
	}
	mutex.Unlock()
	// fmt.Printf("no more idle reduce task")
	return nil
}

func (c* Coordinator) OneMapDone(args *MapArgs, reply *MapReply) error {
	mutex.Lock()
	if args.DoneMf != "" {  // 有任务要提交
		_, ok := c.register[args.Worker]
		if !ok {  // 被炒鱿鱼
			fmt.Printf("fire process %v, map task %v transfered to others, local-deleting\n", args.Worker, args.DoneMf)
			files, _ := ioutil.ReadDir(".")
			for _, file := range files {
				if strings.HasPrefix(file.Name(), "mr-" + fmt.Sprintf("%v-%v", args.DoneMf[3:len(args.DoneMf)-4], args.Worker) + "-") {
					fmt.Printf("deleting %v\n", file.Name())
					os.Remove(filepath.Join(".", file.Name()))					
				}
			}
		} else {
			if c.mapTask[args.DoneMf] {  // 说明是后备任务，否则不会自己还没有被炒鱿鱼就有自己的任务被接替了
				delete(c.register, args.Worker)
				fmt.Printf("late-process %v, backup map task %v done, local-deleting\n", args.Worker, args.DoneMf)
				files, _ := ioutil.ReadDir(".")
				for _, file := range files {
					if strings.HasPrefix(file.Name(), "mr-" + fmt.Sprintf("%v-%v", args.DoneMf[3:len(args.DoneMf)-4], args.Worker) + "-") {
						fmt.Printf("deleting %v\n", file.Name())
						os.Remove(filepath.Join(".", file.Name()))					
					}
				}
			} else {  // 即便是第一次后备任务，其他worker来了也没法影响reduce，因为没有经过master的重命名
				c.mapTask[args.DoneMf] = true
				if c.backup[args.DoneMf] {
					c.backup[args.DoneMf] = false
				}
				delete(c.register, args.Worker)
				fmt.Printf("process %v OneMapDone %v\n", args.Worker, args.DoneMf)
				files, _ := ioutil.ReadDir(".")
				for _, file := range files {
					// os.Rename()不支持带./的，刚好../和pg-都是3个字符且没用，.txt是最后四个也删掉(不过这个好像没有影响)
					if strings.HasPrefix(file.Name(), "mr-" + fmt.Sprintf("%v-%v", args.DoneMf[3:len(args.DoneMf)-4], args.Worker) + "-") {
						fmt.Printf("renaming %v\n", file.Name())
						os.Rename(filepath.Join(".", file.Name()), filepath.Join(".", file.Name()[:len(file.Name())-1]))				
					}
				}
			}
		}
	} 
	// else {  // 没有任务要提交，仅查询是否结束map
	// 	fmt.Printf("process %v AllMapDone? or not?\n", args.Worker)
	// }
	// 无论是否有任务要提交，都需要判断当前所有map是否完成
	for _, done := range c.mapTask {
		if !done {
			mutex.Unlock()
			return nil
		}
	}
	reply.AllDone = true
	mutex.Unlock()
	return nil
}

func (c* Coordinator) OneReduceDone(args *ReduceArgs, reply *ReduceReply) error {
	mutex.Lock()
	// fmt.Printf("OneReduceDone")
	if args.ReduceTask != "" {
		_, ok := c.register[args.Worker]
		if !ok {  // 被炒鱿鱼
			fmt.Printf("fire process %v, reduce task %v transfered to others, local-deleting\n", args.Worker, args.ReduceTask)
			files, _ := ioutil.ReadDir(".")
			for _, file := range files {
				if file.Name() == fmt.Sprintf("mr-out-%v-%v", args.ReduceTask, args.Worker) {
					fmt.Printf("deleting %v\n", file.Name())
					os.Remove(filepath.Join(".", file.Name()))					
				}
			}
		} else {
			c.reduceTask[args.ReduceTask] = true
			delete(c.register, args.Worker)
			fmt.Printf("process %v OneReduceDone %v\n", args.Worker, args.ReduceTask)
			files, _ := ioutil.ReadDir(".")
			for _, file := range files {
				if file.Name() == fmt.Sprintf("mr-out-%v-%v", args.ReduceTask, args.Worker) {
					os.Rename(filepath.Join(".", file.Name()), filepath.Join(".", fmt.Sprintf("mr-out-%v", args.ReduceTask)))				
				}
			}
		}
	} 
	// else {  // 没有任务要提交，仅查询是否结束map
	// 	fmt.Printf("process %v AllReduceDone? or not?\n", args.Worker)
	// }
	for _, done := range c.reduceTask {
		if !done {
			mutex.Unlock()
			return nil
		}
	} 
	reply.AllDone = true
	mutex.Unlock()
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")

	// 移除之前可能存在的同名UNIX域套接字文件
	sockname := coordinatorSock()
	os.Remove(sockname)
  
	// 在指定的UNIX域套接字上监听连接
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	
	// 在后台启动HTTP服务器来处理连接
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	mutex.Lock()
	for _, done := range c.reduceTask {
		if !done {
			mutex.Unlock()
			return false
		}	
	}
	mutex.Unlock()
	
	return ret
}

func (c *Coordinator) UpdateState() {
	mutex.Lock()
	crash := 50 * time.Second
	for xpid, t := range c.register {
		elapse := time.Since(t)
		if elapse >= crash {
			// 删除在册正在执行任务但超时的进程，并清除对应的任务分配
			fmt.Printf("fire the process %v\n", xpid)
			delete(c.register, xpid)
			// 还原该进程未完成的map任务状态
			for mtask, pid := range c.mapAssign {
				if pid == xpid && !c.mapTask[mtask] {
					c.mapAssign[mtask] = -1
				}
			}
			// 还原该进程未完成的reduce任务状态
			for rtask, pid := range c.reduceAssign {
				if pid == xpid && !c.reduceTask[rtask] {
					c.reduceAssign[rtask] = -1
				}
			}
		} else if elapse >= 10 * time.Second {
			for mtask, pid := range c.mapAssign {
				if pid == xpid && !c.mapTask[mtask] && !c.backup[mtask] {
					c.backup[mtask] = true
					fmt.Printf("backup the process %v\n", xpid)
				}
			}
			// // 不处理reduce的backup
			// for rtask, pid := range c.reduceAssign {
			// 	if pid == xpid && !c.reduceTask[rtask] {
			// 		c.reduceAssign[rtask] = -1
			// 	}
			// }
		}
	}
	mutex.Unlock()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTask = make(map[string]bool)
	c.mapAssign = make(map[string]int)
	for _, filename := range files {
		c.mapTask[filename] = false
		c.mapAssign[filename] = -1
	}
	c.nReduce = nReduce
	c.reduceAssign = make(map[string]int)
	c.reduceTask = make(map[string]bool)
	for i := 0; i < nReduce; i++ {
		c.reduceAssign[fmt.Sprintf("%v", i)] = -1
		c.reduceTask[fmt.Sprintf("%v", i)] = false
	}
	c.register = make(map[int]time.Time)
	c.backup = make(map[string]bool)

	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			<- ticker.C
			c.UpdateState()
		}
	}()

	c.server()
	return &c
}
