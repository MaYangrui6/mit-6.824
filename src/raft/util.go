package raft

import (
	"log"
	"sync"
	"sync/atomic"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func ifCond(cond bool, a, b interface{}) interface{} {
	if cond {
		return a
	} else {
		return b
	}
}

// ApplyHelper 结构体的作用是保证 ApplyMsg 按照顺序被提交到 applyCh 进行状态机应用，主要用于 Raft 服务器的日志提交过程。它管理 applyCh 的消息队列 q，确保日志按顺序提交，同时处理快照提交。
type ApplyHelper struct {
	applyCh       chan ApplyMsg // 负责向上层传递应用日志的通道
	lastItemIndex int           // 记录已经提交的最新日志索引
	q             []ApplyMsg    // 维护一个队列，存储等待应用的消息
	mu            sync.Mutex    // 互斥锁，保护 q 和 lastItemIndex
	cond          *sync.Cond    // 用于协调 `applier` 线程等待/唤醒
	dead          int32         // 标志位，判断是否停止
}

func NewApplyHelper(applyCh chan ApplyMsg, lastApplied int) *ApplyHelper {
	applyHelper := &ApplyHelper{
		applyCh:       applyCh,
		lastItemIndex: lastApplied,
		q:             make([]ApplyMsg, 0),
	}
	applyHelper.cond = sync.NewCond(&applyHelper.mu)
	go applyHelper.applier()
	return applyHelper
}

func (applyHelper *ApplyHelper) applier() {
	for !applyHelper.killed() {
		applyHelper.mu.Lock()
		if len(applyHelper.q) == 0 {
			applyHelper.cond.Wait()
		}
		msg := applyHelper.q[0]
		applyHelper.q = applyHelper.q[1:]
		applyHelper.mu.Unlock()
		DPrintf("applyhelper start apply msg index=%v", ifCond(msg.CommandValid, msg.CommandIndex, msg.SnapshotIndex))
		applyHelper.applyCh <- msg
		//<-applyHelper.applyCh
		DPrintf("applyhelper done apply msg index=%v with log entry： %v", ifCond(msg.CommandValid, msg.CommandIndex, msg.SnapshotIndex), msg.Command)
	}
}

func (applyHelper *ApplyHelper) Kill() {
	atomic.StoreInt32(&applyHelper.dead, 1)
}
func (applyHelper *ApplyHelper) killed() bool {
	z := atomic.LoadInt32(&applyHelper.dead)
	return z == 1
}
func (applyHelper *ApplyHelper) tryApply(msg *ApplyMsg) bool {
	applyHelper.mu.Lock()
	defer applyHelper.mu.Unlock()
	DPrintf("applyhelper get msg index=%v", ifCond(msg.CommandValid, msg.CommandIndex, msg.SnapshotIndex))
	if msg.CommandValid {
		if msg.CommandIndex <= applyHelper.lastItemIndex {
			return true
		}
		if msg.CommandIndex == applyHelper.lastItemIndex+1 {
			applyHelper.q = append(applyHelper.q, *msg)
			applyHelper.lastItemIndex++
			applyHelper.cond.Broadcast()
			return true
		}
		panic("applyhelper meet false")
		return false
	} else if msg.SnapshotValid {
		if msg.SnapshotIndex <= applyHelper.lastItemIndex {
			return true
		}
		applyHelper.q = append(applyHelper.q, *msg)
		applyHelper.lastItemIndex = msg.SnapshotIndex
		applyHelper.cond.Broadcast()
		return true
	} else {
		panic("applyHelper meet both invalid")
	}
}
