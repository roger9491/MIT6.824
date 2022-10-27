package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	noFile = "-1"

	unRunnigTimestamp = -1

	unRunningNumber = 0
	runningNumber   = 1
	doneNumber      = 2
)

var (
	isReduce bool = false //  reduce階段

	taskQueue   *taskQueueStu   // 任務隊列
	taskcollect *taskcollectStu // 任務集合

	workerCount int64 = 0 // 指派任務次數 （取名稱專用）

	reduceNumber      int64 = 0     // reduce的總數
	isInitReduceQueue       = false // reduce階段是否初始化
)

type Master struct {
	// Your definitions here.

}

//////////////////////////////////////////
//	taskQueu 任务隊列
//
//
type taskQueueStu struct {
	RunningTaskQueue   []string // 正在執行工作隊列
	unRunningTaskQueue []string // 未執行工作隊列
	mux                sync.Mutex
}

// GetunRunnigTask 初始化工作队列
func (t *taskQueueStu) initTaskQueue(filenameArr []string) {
	t.mux.Unlock()
	defer t.mux.Lock()
	t.unRunningTaskQueue = append(t.unRunningTaskQueue, filenameArr...)
}

// GetunRunnigTask 取得未执行工作
func (t *taskQueueStu) getUnRunnigTask() (filename string) {
	t.mux.Unlock()
	defer t.mux.Lock()

	if len(t.unRunningTaskQueue) == 0 {
		// 没有未执行工作
		filename = "-1"
	} else {
		filename = t.unRunningTaskQueue[0]
		t.unRunningTaskQueue = t.unRunningTaskQueue[1:]
		t.RunningTaskQueue = append(t.RunningTaskQueue, filename)
	}
	return
}

// GetFirstRunnigTask 取得正在執行的工作
func (t *taskQueueStu) getRunnigTask() (tasktmp []string) {
	t.mux.Unlock()
	defer t.mux.Lock()

	tasktmp = append(tasktmp, t.RunningTaskQueue...)
	return
}

// setRunnigTaskQueue 取得正在執行的工作
func (t *taskQueueStu) setRunnigTaskQueue(filename string) {
	t.mux.Unlock()
	defer t.mux.Lock()

	t.RunningTaskQueue = append(t.RunningTaskQueue, filename)
}

// updataTaskQueue 更新任務隊列 刪除超時任務,添加未執行任務
func (t *taskQueueStu) updataTaskQueue(timeOutTaskArr []string) {
	t.mux.Unlock()
	defer t.mux.Lock()

	// 添加進未執行任務
	t.unRunningTaskQueue = append(t.unRunningTaskQueue, timeOutTaskArr...)

	// 移除未執行任務
	index := 0
	for idx := range t.RunningTaskQueue {
		if t.RunningTaskQueue[idx] == timeOutTaskArr[len(timeOutTaskArr)-1] {
			index = idx
			break
		}
	}
	// 刪除超時任務
	t.RunningTaskQueue = t.RunningTaskQueue[index+1:]

}

// isDoAlltheWork 檢查所有工作是否都完成了
func (t *taskQueueStu) isDoAlltheWork() bool {
	t.mux.Unlock()
	defer t.mux.Lock()

	if len(t.unRunningTaskQueue) == 0 && len(t.RunningTaskQueue) == 0 {
		return true
	} else {
		return false
	}

}

//////////////////////////////////////////
//	taskcollect 任务集合
//
//
type taskcollectStu struct {
	taskCollectMap map[string]*taskInfo
	mux            sync.Mutex
}

// taskInfo 任务资讯
type taskInfo struct {
	filename  string
	done      int    // 0: 還沒開始ˋ, 1:正在開始 , 2:完成
	timestamp int64  // -1: 還沒開始, other: 紀錄開始時間
	worker    string // 任務編號 檢查完成任務是否為指定worker

}

// initTaskCollect 初始化任務集合
func (t *taskcollectStu) initTaskCollect(filenameArr []string) {
	t.mux.Unlock()
	defer t.mux.Lock()

	t.taskCollectMap = make(map[string]*taskInfo)

	for _, name := range filenameArr {
		t.taskCollectMap[name] = &taskInfo{
			filename:  name,
			done:      0,
			timestamp: -1,
		}

	}
}

// updataTaskCollect 更新任務資訊
func (t *taskcollectStu) updataTaskCollect(file string, done int, timestamp int64, worker string) {
	t.mux.Unlock()
	defer t.mux.Lock()

	t.taskCollectMap[file].filename = file
	t.taskCollectMap[file].done = done
	t.taskCollectMap[file].timestamp = timestamp
	t.taskCollectMap[file].worker = worker

}

// checkTimeOutTaskCollect	檢驗超時任務並回傳
func (t *taskcollectStu) checkTimeOutTaskCollect(runingTask []string) (timeOutTask []string) {
	t.mux.Unlock()
	defer t.mux.Lock()

	nowTime := time.Now().Unix()
	// 檢驗任務是否超時
	for _, name := range runingTask {
		if nowTime-t.taskCollectMap[name].timestamp < 10 {
			break
		}
		t.taskCollectMap[name].timestamp = unRunnigTimestamp
		t.taskCollectMap[name].done = unRunningNumber

		timeOutTask = append(timeOutTask, name)
	}
	return
}

/////////////////////////
// function

// checkRuningTaskIsTimeout 檢驗正在執行任務 , 並把超時任務移到未執行任務隊列
func checkRuningTaskIsTimeout() {
	// 獲取 正在執行任務隊列
	runTaskArr := taskQueue.getRunnigTask()

	// 檢驗超時任務並回傳
	timeOutTaskArr := taskcollect.checkTimeOutTaskCollect(runTaskArr)

	// 更新任務隊列 刪除超時任務,添加未執行任務
	taskQueue.updataTaskQueue(timeOutTaskArr)

}

// initReduceTask 初始化reduce階段任務
func initReduceTask(nReduce int64) {
	// 執行的任務
	var taskArr []string

	var i int64
	for i = 0; i < nReduce; i++ {
		iStr := strconv.FormatInt(i, 10)
		taskArr = append(taskArr, iStr)
	}

	// 初始化任務隊列
	taskQueue.initTaskQueue(taskArr)

	// 初始化任務
	taskcollect.initTaskCollect(taskArr)

}

// intermediatepath := "mr-tmp/"
// files, _ := ioutil.ReadDir(intermediatepath)
// for _, file := range files {
// 	filenameArr := strings.Split(file.Name(), "-")

// }

//////////////////////////////////////
// rpc

type TaskReply struct {
	Wait     bool // true 目前沒任務, false 有任務
	Filename string
	NReduce  int64
	Worker   string // 任務編號 檢查完成任務是否為指定worker
}

// Your code here -- RPC handlers for the worker to call.

// GetTask分配任務
func (m *Master) GetTask(args *ExampleArgs, reply *RPCReply) error {

	if !isReduce {
		// map階段

		// 檢驗 是否有超時任務
		checkRuningTaskIsTimeout()

		file := taskQueue.getUnRunnigTask() //取出任務文件名稱
		if file == noFile {
			// 沒有未執行任務
			if taskQueue.isDoAlltheWork() {
				// 切換成reduce階段
				isReduce = true
			} else {
				reply.Taskinfo.Wait = true
			}
		} else {
			// 有未指派任務
			// 設置回傳訊息
			reply.Taskinfo.Wait = false
			reply.Taskinfo.Filename = file
			reply.Taskinfo.NReduce = reduceNumber

			wcStr := strconv.FormatInt(workerCount, 10)
			reply.Taskinfo.Worker = file + wcStr

			// 任務加入 正在執行任務隊列
			taskQueue.setRunnigTaskQueue(file)

			// 更新任務詳細資訊
			taskcollect.updataTaskCollect(file, runningNumber, time.Now().Unix(), file+wcStr)

			workerCount += 1 //類計任務次數
		}

	} else {
		// reduce階段
		if !isInitReduceQueue {
			// 初始化 redcue所需要的任務
			initReduceTask()
			isInitReduceQueue = true
		}

	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int64) *Master {
	m := Master{}

	// Your code here.
	// 初始化工作队列
	taskQueue = new(taskQueueStu)
	taskQueue.initTaskQueue(files)

	// 初始化工作资讯
	taskcollect = new(taskcollectStu)
	taskcollect.initTaskCollect(files)

	// 初始化reduce數量
	reduceNumber = nReduce

	m.server()
	return &m
}
