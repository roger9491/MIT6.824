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

	reduceNumber      int = 0     // reduce的總數
	mapCount          int = 0     // map數量
	isInitReduceQueue     = false // reduce階段是否初始化

	isDone = false
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
	t.mux.Lock()
	defer t.mux.Unlock()
	t.unRunningTaskQueue = filenameArr
}

// getRunnigTaskQueue 取得正在執行的工作隊列
func (t *taskQueueStu) getRunnigTaskQueue() (tasktmp []string) {
	tasktmp = append(tasktmp, t.RunningTaskQueue...)
	return
}

// updataTaskQueue 更新任務隊列 刪除超時任務,添加未執行任務
func (t *taskQueueStu) updataTaskQueue(timeOutTaskArr []string) {
	t.mux.Lock()
	defer t.mux.Unlock()

	if len(timeOutTaskArr) != 0 {

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
}

//	updataUnRunningTaskQueue 刪除已完成任務
func (t *taskQueueStu) updataRunningTaskQueue(file string) {
	t.mux.Lock()
	defer t.mux.Unlock()

	// 從正在執行任務移除
	var filetmp []string

	for _, name := range t.RunningTaskQueue {
		if name != file {
			filetmp = append(filetmp, name)
		}
	}

	t.RunningTaskQueue = filetmp
}

// assignTask 回傳任務, 並移動 任務 到 執行隊列
func (t *taskQueueStu) assignTask() (taskName string) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if len(t.unRunningTaskQueue) == 0 {
		taskName = "-1"
		return
	}
	taskName = t.unRunningTaskQueue[0]

	t.unRunningTaskQueue = t.unRunningTaskQueue[1:]
	t.RunningTaskQueue = append(t.RunningTaskQueue, taskName)

	return
}

// isDoAlltheWork 檢查所有工作是否都完成了
func (t *taskQueueStu) isDoAlltheWork() bool {
	t.mux.Lock()
	defer t.mux.Unlock()

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
	mapNumber int    // map編號 （命名中間文件所需）
}

// initTaskCollect 初始化任務集合
func (t *taskcollectStu) initTaskCollect(filenameArr []string) {
	t.mux.Lock()
	defer t.mux.Unlock()

	t.taskCollectMap = make(map[string]*taskInfo)

	for idx, name := range filenameArr {
		t.taskCollectMap[name] = &taskInfo{
			filename:  name,
			done:      0,
			timestamp: -1,
			mapNumber: idx,
		}

	}
}

// getTaskMapByFileName 藉由文件名稱取得任務資訊
func (t *taskcollectStu) getTaskMapByFileName(fileName string) (task taskInfo) {
	t.mux.Lock()
	defer t.mux.Unlock()

	task.done = t.taskCollectMap[fileName].done
	task.filename = t.taskCollectMap[fileName].filename
	task.mapNumber = t.taskCollectMap[fileName].mapNumber
	task.timestamp = t.taskCollectMap[fileName].timestamp
	task.worker = t.taskCollectMap[fileName].worker

	return
}

// updataTaskCollect 更新任務資訊
func (t *taskcollectStu) updataTaskCollect(file string, done int, timestamp int64, worker string) {
	t.mux.Lock()
	defer t.mux.Unlock()

	t.taskCollectMap[file].filename = file
	t.taskCollectMap[file].done = done
	t.taskCollectMap[file].timestamp = timestamp
	t.taskCollectMap[file].worker = worker

}

// checkTimeOutTaskCollect	檢驗超時任務並回傳
func (t *taskcollectStu) checkTimeOutTaskCollect(runingTask []string) (timeOutTask []string) {
	t.mux.Lock()
	defer t.mux.Unlock()

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

	tmpRunningTask := taskQueue.getRunnigTaskQueue()

	// 檢驗超時任務並回傳
	timeOutTaskArr := taskcollect.checkTimeOutTaskCollect(tmpRunningTask)

	// 更新任務隊列 刪除超時任務,添加未執行任務
	taskQueue.updataTaskQueue(timeOutTaskArr)

}

// initReduceTask 初始化reduce階段任務
func initReduceTask(nReduce int) {
	// 執行的任務
	var taskArr []string

	for i := 0; i < nReduce; i++ {
		iStr := strconv.Itoa(i)
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
	Wait      bool // true 目前沒任務, false 有任務
	Filename  string
	NReduce   int
	MapNumber int    // map編號
	Worker    string // 任務編號 檢查完成任務是否為指定worker
	IsReduce  bool   // 使否為reduce階段

}

type TaskArgs struct {
	Filename  string
	Worker    string // 任務編號 檢查完成任務是否為指定worker
	Timestamp int64  // 任務必須10秒內完成
}

// Your code here -- RPC handlers for the worker to call.

// GetTask分配任務
func (m *Master) GetTask(args *RPCArgs, reply *RPCReply) error {

	// 任務排程算法
	// 檢驗 是否有超時任務
	checkRuningTaskIsTimeout()

	file := taskQueue.assignTask() // 取得分配任務

	// 沒有未分配任務
	if file == noFile {

		// 判斷是否都做完任務
		if taskQueue.isDoAlltheWork() {
			// 判斷當前階段
			if isReduce {
				// 當前為reduce階段
				// 沒有任務,任務完成 程式結束
				isDone = true
				log.Fatal("-----任務完成-----")
			} else {
				// 切換成reduce階段
				isReduce = true

				// reduce階段
				if !isInitReduceQueue {
					// 初始化 redcue所需要的任務
					initReduceTask(reduceNumber)
					isInitReduceQueue = true
				}

				file = taskQueue.assignTask() // 取得分配任務
			}
		} else {
			// 沒有空閒任務 讓worker等待
			reply.Taskinfo.Wait = true
			return nil
		}
	}
	// 獲取任務詳細資訊

	fileInfo := taskcollect.getTaskMapByFileName(file)

	// 設置回傳訊息
	reply.Taskinfo.Wait = false
	reply.Taskinfo.Filename = file
	reply.Taskinfo.NReduce = reduceNumber

	wcStr := strconv.FormatInt(workerCount, 10)
	reply.Taskinfo.Worker = file + wcStr

	// reduce階段
	if isReduce {
		reply.Taskinfo.IsReduce = true
	} else {
		reply.Taskinfo.IsReduce = false
	}

	if isReduce {
		// reduce 階段
		reply.Taskinfo.MapNumber = mapCount // 文件的數量
	} else {
		// 分配任務編號
		reply.Taskinfo.MapNumber = fileInfo.mapNumber // 文件編號
	}

	// 更新任務詳細資訊
	taskcollect.updataTaskCollect(file, runningNumber, time.Now().Unix(), file+wcStr)

	workerCount += 1 //類計任務次數

	return nil
}

// TaskDone 完成任務
func (m *Master) TaskDone(args *RPCArgs, reply *RPCReply) error {

	// 取得任務資訊
	fileInfo := taskcollect.getTaskMapByFileName(args.Taskinfo.Filename)

	// 必須是同一個worker && 必須在10內完成
	if fileInfo.worker == args.Taskinfo.Worker && args.Taskinfo.Timestamp-fileInfo.timestamp <= 10 {
		// 跟指派任務的worker是一樣的

		// 從正在執行任務隊列 移除 已完成任務
		taskQueue.updataRunningTaskQueue(args.Taskinfo.Filename)

		// 更新任務資訊
		taskcollect.updataTaskCollect(args.Taskinfo.Filename, doneNumber, unRunnigTimestamp, args.Taskinfo.Worker)

	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *RPCArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

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
	ret := isDone
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{}

	// 設置日誌輸出配置

	f, err := os.OpenFile("./mrmaster_log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		// test
		log.Fatal("asdad asd")
		return &m
	}

	defer f.Close()

	log.SetOutput(f)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Println("test test ")
	// Your code here.
	// 初始化工作队列
	taskQueue = new(taskQueueStu)
	taskQueue.initTaskQueue(files)

	// 初始化工作资讯
	taskcollect = new(taskcollectStu)
	taskcollect.initTaskCollect(files)

	// 初始化reduce數量
	reduceNumber = nReduce

	// 初始化map數量
	mapCount = len(files)

	m.server()
	return &m
}
