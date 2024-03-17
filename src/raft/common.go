package raft

import (
	"time"
)

// UpdateHeartTime 需要在同步代码块中执行
func (rf *Raft) updateHeartTime() {
	if t := time.Now(); t.After(rf.lastHeartTime) {
		// 重置超时选举时间
		rf.lastHeartTime = t
	}
}

// 需要在同步代码块中执行
func (rf *Raft) updateTerm(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.persist()
}

// 需要在同步代码块中执行
func (rf *Raft) trimEntry(sliceIdx int) {
	if len(rf.log)-1 <= sliceIdx {
		rf.log = make([]Entry, 1)
		rf.log[0].Term = rf.snapshotTerm
		return
	}
	tmp := make([]Entry, 1)
	tmp = append(tmp, rf.log[sliceIdx+1:]...)
	rf.log = tmp
	rf.log[0].Term = rf.snapshotTerm // log[0]为占位entry，方便 AE rpc 和 RV rpc 的term比较
}

// 需要在同步代码块中执行
func (rf *Raft) toSliceIndex(realIndex int) int {
	return realIndex - rf.snapshotIndex
}

// 需要在同步代码块中执行
func (rf *Raft) toRealIndex(sliceIndex int) int {
	return sliceIndex + rf.snapshotIndex
}
