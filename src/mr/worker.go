package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

const (
	imdDir     = "tmp/" // 中間件儲存目錄
	imdPreName = "xy-"     // 中間件文件名稱: "xy-MapNumber-ihash(key) % NReduce"
	splitStr   = "-"
	outNamePre = "mr-out-" // 輸出文件前綴名
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
			
	// 設置日誌輸出配置
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	f, err := os.OpenFile("./mrworker_log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return
	}

	defer f.Close()

	log.SetOutput(f)

	// 建立中間件文件夾
	err = os.Mkdir(imdDir, os.ModePerm)
	if err != nil {
		log.Println(err)
	}

	for {
		// 向 master 請求任務
		args := RPCArgs{}
		reply := RPCReply{}

		// type TaskReply struct {
		// 	Wait        bool
		// 	Filename    string
		// 	NReduce        int
		// 	mapNumber    int
		// 	Worker        string
		// 	IsReduce    bool
		// }
		err := call("Master.GetTask", &args, &reply)
		if !err {
			log.Fatal(" master 失去響應 , 工作已完成 ")
		}
	

		if reply.Taskinfo.Wait {
			// 沒有空閒任務
			time.Sleep(1 * time.Second)
		} else {
			// 執行任務
			// 判斷是否為˙reduce 階段

			if !reply.Taskinfo.IsReduce {
				// map 階段

				filename := reply.Taskinfo.Filename
				// use mrsequential code
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()

				// 轉成中間件
				kva := mapf(filename, string(content)) // collection of all letters

				// 寫入暫存文件, 避免因崩潰產生錯誤資訊寫入文件
				/*
					中間件分類按	ihash(key) % NReduce
					每個 worker至多產生nreduce 個文件

				*/
				var tmpFileArr []*os.File // nreduece個文件 索引值區分
				for i := 0; i < reply.Taskinfo.NReduce; i++ {
					tmpFile, err := ioutil.TempFile("./mr-tmp", reply.Taskinfo.Worker)
					if err != nil {
						log.Fatal("Cannot create temporary file", err)
					}
					tmpFileArr = append(tmpFileArr, tmpFile)
				}

				// 寫入暫存文件
				for _, kv := range kva {
					// 判斷屬於哪個nreduce
					index := ihash(kv.Key) % reply.Taskinfo.NReduce
					// 建立指定文件對象
					enc := json.NewEncoder(tmpFileArr[index])
					// 寫入
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal("woker json 寫入錯誤", err)
					}
				}

				mapNumberStr := strconv.Itoa(reply.Taskinfo.MapNumber)

				// rename 文件
				for idx, tmpFile := range tmpFileArr {
					idxStr := strconv.Itoa(idx)
					os.Rename(tmpFile.Name(), imdDir+imdPreName+mapNumberStr+splitStr+idxStr)
					log.Println(imdDir+imdPreName+mapNumberStr+splitStr+idxStr)
					// 刪除佔存檔
					os.Remove(tmpFile.Name())
				}
			} else {
				// reduce 階段
				// 任務名稱為 reduce編號

				var intermediate []KeyValue
				// 取出 單詞
				for i := 0; i < reply.Taskinfo.MapNumber; i++ {
					iStr := strconv.Itoa(i)
					fileName := imdDir + imdPreName + iStr + splitStr + reply.Taskinfo.Filename

					// 打開文件
					filePtr, err := os.Open(fileName)
					if err != nil {
						log.Fatal("打開文件失敗 ", err)
					}
					dec := json.NewDecoder(filePtr)
					// 取出單詞
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}

				sort.Sort(ByKey(intermediate))

				// 合併計算, 並輸出至文件 "mr-out-reduceNumber"
				oname := outNamePre + reply.Taskinfo.Filename
				ofile, _ := os.Create(oname)

				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// 寫入文件
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}

				ofile.Close()

			}

			// 完成任務
			args = RPCArgs{}
			args.Taskinfo = TaskArgs{
				Filename: reply.Taskinfo.Filename,
				Worker:   reply.Taskinfo.Worker,
				Timestamp: time.Now().Unix(),
			}
			reply = RPCReply{}
			errbool := call("Master.TaskDone", &args, &reply)
			if !errbool {
				log.Fatal(" master 失去響應 , 工作已完成 ")
			}

		}

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
