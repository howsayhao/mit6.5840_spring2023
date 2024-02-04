package mr

import (
	"encoding/json"
	"fmt"
	// "path/filepath"

	// "hash"
	"hash/fnv"

	// "io"
	"bufio"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
var NReduce int

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// for sorting by key.
type ByKeyHash []KeyValue

// for sorting by key.
func (a ByKeyHash) Len() int           { return len(a) }
func (a ByKeyHash) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKeyHash) Less(i, j int) bool { return ihash(a[i].Key) % NReduce < ihash(a[j].Key) % NReduce }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var AllDone bool
	// 轮询直到coordinator所有map任务都被完成
	turn := 0
	for !AllDone {
		// 申请并完成被分配的map任务
		reply := MapReply{}
		MapTaskWorker(mapf, &reply, turn)
		// 每过几秒向coordinator询问空闲map任务
		time.Sleep(time.Second)
		// args := MapArgs{}
		// reply := MapReply{}
		// ok := call("Coordinator.MapDone", &args, &reply)
		// if ok {
			AllDone = reply.AllDone
			turn++
		// } else {
		// 	log.Fatalf("fail to call MapDone")
		// }
		fmt.Printf("next turn\n")
	}
	fmt.Printf("all map task done!\n")
	
	// 轮询直到所有reduce任务也被解决
	AllDone = false
	for !AllDone {
		reply := ReduceReply{}
		ReduceTaskWorker(reducef, &reply)

		time.Sleep(time.Second)
		// args := ReduceArgs{}
		// reply := ReduceReply{}
		// ok := call("Coordinator.ReduceDone", &args, &reply)
		// if ok {
			AllDone = reply.AllDone
		// } else {
			// log.Fatalf("fail to call ReduceDone")
		// }
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func MapTaskWorker(mapf func(string, string) []KeyValue, doneReply *MapReply, turn int) {
	// 函数体内包含两次call通讯，但因为同一进程的顺序执行，只有不同进程的call调用才可能会带来共享内存数据冲突的问题
	args := MapArgs{}
	args.Worker = os.Getpid()
	reply := MapReply{}
	ok := call("Coordinator.HandleRequest", &args, &reply)

	if ok && reply.Mf != "" {
		NReduce = reply.NReduce
		fmt.Printf("map task is file %v\n", reply.Mf)
		intermediate := []KeyValue{}

		// 读取文件内容到数组[]KeyValue中
		filename := reply.Mf
		file, _ := os.Open(filename)
		content, _ := ioutil.ReadAll(file)
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)

		// 将[]KeyValue写入对应文件中
		sort.Sort(ByKeyHash(intermediate))
		i := 0
		var ofile *os.File
		for i < len(intermediate) {
			hashgroup := ihash(intermediate[i].Key) % NReduce
			ofilename := fmt.Sprintf("mr-%v-%v-", fmt.Sprintf("%v%v", args.Worker, turn), hashgroup)
			if _, err := os.Stat(ofilename); os.IsNotExist(err) {
				ofile, _ = os.Create(ofilename)
			} else {
				ofile, _ = os.OpenFile(ofilename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			}

			j := i + 1
			for j < len(intermediate) && ihash(intermediate[j].Key) % NReduce == hashgroup {
				j ++
			}
			enc := json.NewEncoder(ofile)
			buf := bufio.NewWriter(ofile)
			for k := i; k < j; k++ {
				enc.Encode(&intermediate[k])  // json化键值对并写入文件
			}
			buf.Flush() // 将缓冲区的内容一次性写入文件
			ofile.Close()
			i = j 
		}
		args.DoneMf = reply.Mf
		args.Turn = turn
		ok := call("Coordinator.OneMapDone", &args, &doneReply)
		if !ok {
			log.Fatalf("fail to call OneMapDone")
		}
		fmt.Printf("map task %v done, waiting for submiting ack\n", reply.Mf)
	} else {
		args.DoneMf = ""
		ok := call("Coordinator.OneMapDone", &args, &doneReply)
		if !ok {
			log.Fatalf("fail to call OneMapDone2")
		}
	}
}

func ReduceTaskWorker(reducef func(string, []string) string, doneReply *ReduceReply) {
	args := ReduceArgs{}
	args.Worker = os.Getpid()
	reply := ReduceReply{}
	ok := call("Coordinator.HandleReduce", &args, &reply)

	if ok && reply.Reducer != "" {
		fmt.Printf(" reduce task is file mr-out-%v\n", reply.Reducer)
		files, _ := ioutil.ReadDir(".")
		// 多个文件可能包含相同的键值对，因而不止需要对单个中间文件归约，而是所有面向该reducer的文件归约
		ofile, _ := ioutil.TempFile(".", "tempFile" + reply.Reducer)
		var kva []KeyValue
		for _, file := range files {
			if strings.HasSuffix(file.Name(), "-" + reply.Reducer) && !strings.HasSuffix(file.Name(), "out-" + reply.Reducer) {
				itfile, _ := os.Open(file.Name())
				dec := json.NewDecoder(itfile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				itfile.Close()
			}
		}
		sort.Sort(ByKey(kva))
		for i := 0; i < len(kva); {
			j := i + 1
			for j < len(kva) && kva[i].Key == kva[j].Key {
				j ++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			i = j
		}
		os.Rename(ofile.Name(), "mr-out-" + reply.Reducer + fmt.Sprintf("-%v", args.Worker))
		ofile.Close()
		args.ReduceTask = reply.Reducer
		ok := call("Coordinator.OneReduceDone", &args, &doneReply)
		if !ok {
			log.Fatalf("fail to call OneReduceDone")
		}
	} else {
		args.ReduceTask = ""
		ok := call("Coordinator.OneReduceDone", &args, &doneReply)
		if !ok {
			log.Fatalf("fail to call OneReduceDone2")
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
