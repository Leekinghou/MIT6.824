package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义为全局，worker之间访问coordinator时加锁
var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReducerNumber     int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	ReduceTaskChannel chan *Task     // 使用chan保证并发安全
	MapTaskChannel    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组
}

// TaskMetaInfo 用于存放task的元信息
type TaskMetaInfo struct {
	state     State     // 任务的状态
	StartTime time.Time // 任务的开始时间，为crash做准备
	TaskAddr  *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// TaskMetaHolder 其中taskMetaHolder为存放全部元信息(TaskMetaInfo)的map，当然用slice也行
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 用于存放全部的task的元信息
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:             files,
		ReducerNumber:     nReduce,
		DistPhase:         MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			// 任务的总数应该是files + Reducer的数量
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	c.makeMapTasks(files)

	c.server()

	// 因此我们对crash的情况可以大概这样处理：先给在工作信息补充一个记录时间的开始状态，
	// 然后在初始化协调者的时候同步开启一个crash探测协程，将超过10s的任务都放回chan中，等待任务重新读取。
	go c.CrashDetector()

	return &c
}

// makeMapTasks 初始化map任务。将Map任务放到Map管道中，taskMetaInfo放到taskMetaHolder中
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNumber,
			FileSlice:  []string{v},
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:    Waiting, // 任务等待被执行
			TaskAddr: &task,   // 保存任务的地址
		}

		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		// 将任务放到Map管道中
		fmt.Printf("[%s] make a [map] task Task-ID: [%d]\n", time.Now().Format("2006-01-02 15:04:05"),
			task.TaskId)
		//fmt.Println("make a map task: ", &task)
		c.MapTaskChannel <- &task
	}
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working {
				//fmt.Println("task[", v.TaskAddr.TaskId, "] is working: ", time.Since(v.StartTime), "s")
			}

			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("任务 [%d] 崩溃，耗时 [%f] 秒\n", v.TaskAddr.TaskId, time.Since(v.StartTime).Seconds())

				switch v.TaskAddr.TaskType {
				case MapTask:
					c.MapTaskChannel <- v.TaskAddr
					v.state = Waiting
				case ReduceTask:
					c.ReduceTaskChannel <- v.TaskAddr
					v.state = Waiting
				}
			}
		}
		mu.Unlock()
	}
}

// makeReduceTasks 初始化reduce任务
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNumber; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:    Waiting, // 任务等待被执行
			TaskAddr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		//fmt.Println("make a reduce task :", &task)
		c.ReduceTaskChannel <- &task
	}
}

// PollTask 核心的方法: 分配任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// 1. 将map任务管道中的任务取出
	// 分发任务应该上锁，防止多个worker竞争，并用defer回退解锁
	mu.Lock()
	defer mu.Unlock()

	// 判断任务类型，存任务
	switch c.DistPhase {
	case MapPhase:
		if len(c.MapTaskChannel) > 0 {
			*reply = *<-c.MapTaskChannel
			// fmt.Printf("poll-Map-taskid[ %d ]\n", reply.TaskId)
			if !c.taskMetaHolder.judgeState(reply.TaskId) {
				fmt.Printf("[Map] task Task-id[%d] is running\n", reply.TaskId)
			}
		} else {
			// 如果map任务被分发完了但是又没完成，此时就将任务设为WaitTask
			reply.TaskType = WaitTask
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(c.ReduceTaskChannel) > 0 {
			*reply = *<-c.ReduceTaskChannel
			//fmt.Printf("poll-Reduce-taskid[ %d ]\n", reply.TaskId)
			if !c.taskMetaHolder.judgeState(reply.TaskId) {
				fmt.Printf("[Reduce] task Task-id[%d] is running\n", reply.TaskId)
			}
		} else {
			// 如果map任务被分发完了但是又没完成，此时就将任务设为WaitTask
			reply.TaskType = WaitTask
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case AllDone:
		reply.TaskType = ExitTask
	default:
		panic("The phase undefined ! ! !")
	}
	return nil
}

// 将接受taskMetaInfo储存进MetaHolder里
func (t *TaskMetaHolder) acceptMeta(taskInfo *TaskMetaInfo) bool {
	taskId := taskInfo.TaskAddr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Printf("[%s] task already exist, existed id is: [%d]\n",
			time.Now().Format("2006-01-02 15:04:05"),
			taskId)
		return false
	} else {
		//fmt.Printf("[%s] accept a new task, task-id is: [%d]\n",
		//	time.Now().Format("2006-01-02 15:04:05"),
		//	taskId)
		t.MetaMap[taskId] = taskInfo
		return true
	}
}

// 拼接字符串：读文件名
func selectReduceName(reduceNum int) []string {
	var s []string
	path, err := os.Getwd()
	if err != nil {
		panic("path error")
	}
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		panic("read dir error")
	}
	for _, file := range dir {
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, file.Name())
		}
	}
	return s
}

// 分配任务中转换阶段
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// 检查多少个任务做了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum       = 0
		mapNotDoneNum    = 0
		reduceDoneNum    = 0
		reduceNotDoneNum = 0
	)

	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		// 首先判断任务的类型
		if v.TaskAddr.TaskType == MapTask {
			// 判断任务是否完成,下同
			if v.state == Done {
				mapDoneNum++
			} else {
				mapNotDoneNum++
			}
		} else if v.TaskAddr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceNotDoneNum++
			}
		}

	}
	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true

	// Map
	//if (mapDoneNum > 0 && mapNotDoneNum == 0) && (reduceDoneNum == 0 && reduceNotDoneNum == 0) {
	//	return true
	//} else {
	//	if reduceDoneNum > 0 && reduceNotDoneNum == 0 {
	//		return true
	//	}
	//}
	return (mapDoneNum > 0 && mapNotDoneNum == 0 && reduceDoneNum == 0 && reduceNotDoneNum == 0) ||
		(reduceDoneNum > 0 && reduceNotDoneNum == 0)
}

// 判断给定任务是否在工作，并修正其目前任务信息状态,如果任务不在工作的话返回true
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

// Example an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	newReply := ExampleReply{
		1000,
	}
	reply = &newReply
	fmt.Println("example")
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

// MarkFinished 调用的rpc方法，将任务标记为完成
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
	if ok && meta.state == Working {
		meta.state = Done
		//fmt.Printf("[%v] %v task[%d] is done\n", time.Now().Format("2006-01-02 15:04:05"), taskTypeToString(MyEnum(args.TaskType)), args.TaskId)
	} else {
		fmt.Printf("[%v] undefined task[%d] is already done\n", time.Now().Format("2006-01-02 15:04:05"), args.TaskId)
	}
	return nil
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished. 如果map任务全部实现完，阶段为AllDone那么Done方法应该返回true,使Coordinator能够exit程序。
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		//fmt.Printf("All tasks has been done! The coordinator will be exit\n")
		ret = true
	}
	return ret
}
