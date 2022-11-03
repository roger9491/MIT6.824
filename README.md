這是 Mit 6.824 課程程式碼

課程連結: http://nil.csail.mit.edu/6.824/2020/index.html

下載此專案程式碼

``` 
git clone git://g.csail.mit.edu/6.824-golabs-2020 6.824
```



目前已完成

​	Lab1 Mapreduce



## Lab1 Mapreduce

Lab1 課程說明: http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html

此實驗要求我們實現 論文中提到的範例 "Word Count" , 也就是 計數單詞個數，利用論文中提到的分佈式架構來實現。

實驗要求我們把程式碼寫在 master.go, rpc.go, worker.go

https://github.com/roger9491/MIT6.824/tree/master/src/mr

master.go : 主要式分配任務

rpc.go : 定義 rpc 參數結構

worker.go : 主要為 輸入文件，並拆分單詞，最後在合併單詞並輸出。

### 程式流程

啟動 master , 等待 worker 請求任務

多個 worker 啟動，並向 master 請求任務

master 根據當前階段指派適合的任務, 並定期檢查是否有 worker 超時

當任務全部完成 master 跳出, worker 呼叫不到master 也結束。

