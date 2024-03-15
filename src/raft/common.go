package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) loadState() State {
	return State(atomic.LoadInt32((*int32)(&rf.state)))
}

func (rf *Raft) storeState(s State) {
	atomic.StoreInt32((*int32)(&rf.state), int32(s))
}

// 需要在同步代码块中执行
func (rf *Raft) updateTime() {
	if t := time.Now(); t.After(rf.lastHeartTime) {
		// 重置超时选举时间
		rf.lastHeartTime = t
	}
}

// 需要在同步代码块中执行
func (rf *Raft) updateTerm(term int) {
	rf.storeState(FOLLOWER)
	rf.currentTerm = term
	rf.persist() // 持久化数据
}

// 需要在同步代码块中执行
func (rf *Raft) trimLog(sliceIdx int) {
	tmp := make([]Entry, 1, Max(len(rf.log)-sliceIdx, 1))
	if len(rf.log)-sliceIdx > 1 {
		tmp = append(tmp, rf.log[sliceIdx+1:]...)
	}
	rf.log = tmp
	rf.log[0].Term = rf.snapshotTerm // log[0]为占位entry，方便 AE rpc 和 RV rpc 的term比较
}

// 需要在同步代码块中执行
func (rf *Raft) toSliceIndex(logIndex int) int {
	return logIndex - rf.snapshotIndex
}

// 需要在同步代码块中执行
func (rf *Raft) toLogIndex(sliceIndex int) int {
	return sliceIndex + rf.snapshotIndex
}
