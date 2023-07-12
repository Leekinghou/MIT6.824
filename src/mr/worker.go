package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// SortedKey 实现了sort.Interface接口中的Len、Less和Swap方法，用于按照KeyValue中的Key进行排序
type SortedKey []KeyValue

func (sk SortedKey) Len() int {
	return len(sk)
}

func (sk SortedKey) Less(i, j int) bool {
	return sk[i].Key < sk[j].Key
}

func (sk SortedKey) Swap(i, j int) {
	sk[i], sk[j] = sk[j], sk[i]
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	keepFlag := true
	for keepFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			DoMapTask(&task, mapf)
			callDone(&task)
		case WaitTask:
			time.Sleep(time.Second * 5)
		case ReduceTask:
			DoReduceTask(&task, reducef)
			callDone(&task)
		case ExitTask:
			time.Sleep(time.Second)
			fmt.Println("All tasks are done, exit...")
			keepFlag = false
		}
	}
	time.Sleep(time.Second)
}

// GetTask 调用RPC拉取coordinator的任务，需要知道是Map任务，还是Reduce
func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	flag := call("Coordinator.PollTask", &args, &reply)
	if flag {
		//fmt.Printf("[%v] worker get [%v] task-Id:[%d]\n", time.Now().Format("2006-01-02 15:04:05"),
		//	taskTypeToString(MyEnum(reply.TaskType)), reply.TaskId)
		//fmt.Printf(reply.FileSlice[0])
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func DoMapTask(response *Task, mapf func(string, string) []KeyValue) {
	// 存储映射函数的输出结果
	var intermediate []KeyValue

	filename := response.FileSlice[0]
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v: %v", filename, err)
	}

	intermediate = mapf(filename, string(content))

	//将map的结果写入中间文件
	//initialize and loop over []KeyValue
	num := response.ReducerNum
	HashedKV := make([][]KeyValue, num)

	// 遍历intermediate中的键值对，根据键的哈希值计算出对应的index，将键值对追加到HashedKV的对应位置。
	for _, kv := range intermediate {
		//hash the key and get the index of the reducer
		index := ihash(kv.Key) % num
		HashedKV[index] = append(HashedKV[index], kv)
	}

	// 循环num次，为每个哈希值计算出的index创建一个中间文件，并将对应位置的键值对写入到该文件中，用于解决crash问题
	for i := 0; i < num; i++ {
		// create intermediate file
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create intermediate file %s: %v", oname, err)
		}
		//write to intermediate file
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("encode error for intermediate file %s: %v", oname, err)
				//return
			}
		}
		//close intermediate file
		if err := ofile.Close(); err != nil {
			log.Fatalf("cannot close intermediate file %s: %v", oname, err)
		}
		//send intermediate file to coordinator
	}
}

// DoReduceTask 分配reduce任务，跟map一样参考wc.go、mrsequential.go方法
// 思路：对之前的tmp文件进行洗牌（shuffle），得到一组排序好的kv数组,并根据重排序好kv数组重定向输出文件。
func DoReduceTask(t *Task, reducef func(string, []string) string) {
	reduceFileNum := t.TaskId
	intermediate := shuffle(t.FileSlice)
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("Failed to crete temp file", err)
	}
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to crete temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	// 在完全写入后进行重命名
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

// shuffle 作用是将输入的文件列表中的所有文件读取并解析成KeyValue类型，然后对这些KeyValue按照Key进行排序，并返回排序后的KeyValue列表。
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	// 遍历文件列表
	for _, filePath := range files {
		file, _ := os.Open(filePath)
		// 打开文件并使用json.Decoder解码其中的KeyValue类型的数据
		dec := json.NewDecoder(file)
		// 添加到一个KeyValue类型的切片中
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// 对切片进行排序
	sort.Sort(SortedKey(kva))
	return kva
}

// callDone Call RPC to mark the task as completed
// 做完任务也需要调用rpc在coordinator中将任务状态为设为已完成，以方便coordinator确认任务已完成，worker与协调者程序能正常退出。
func callDone(f *Task) Task {
	args := f
	reply := Task{}
	flag := call("Coordinator.MarkFinished", &args, &reply)
	if flag {
		fmt.Printf("[%s] Finished: Task-Type [%s] Task-ID: [%d]\n", time.Now().Format("2006-01-02 15:04:05"),
			taskTypeToString(MyEnum(reply.TaskType)), args.TaskId)
	} else {
		fmt.Println("call failed!")
	}
	return reply
}

// CallExample example function to show how to make an RPC call to the coordinator. 以显示如何对coordinator进行RPC调用。
// the RPC argument and reply types are defined in rpc.go. RPC参数和应答类型在rpc.go中被定义。
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
	// the Example() method of struct Coordinator. “Coordinator.Example”告诉接收服务器，我们想调用Coordinator结构体的Example()方法。
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
