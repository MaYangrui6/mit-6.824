package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	// 检查命令行参数是否足够
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	// 创建一个 MapReduce Coordinator，处理输入文件
	m := mr.MakeCoordinator(os.Args[1:], 10)

	// 持续检查任务是否完成
	for m.Done() == false {
		time.Sleep(time.Second) // 休眠 1 秒，避免 CPU 过载
	}

	// 额外等待 1 秒后结束
	time.Sleep(time.Second)
}
