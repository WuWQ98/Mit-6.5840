package raft

import (
	"time"
)

// 需要在同步代码块中执行
func (rf *Raft) updateNextIndex() {
	// 制作snapshot的时间是随机的，制作snapshot后，nextIndex-snapshotIndex 有可能小于等于0
	// 或者AE日志匹配失败后，计算的 nextIndex-snapshotIndex 有可能小于等于0
	// 若 nextIndex-snapshotIndex <= 0，则取nextIndex = snapshotIndex+1
	for i := range rf.nextIndex {
		if i != rf.me {
			rf.nextIndex[i] = Max(rf.nextIndex[i], rf.snapshotIndex)
		} else {
			rf.nextIndex[i] = rf.toRealIndex(len(rf.log))
		}
	}
}

// 需要在同步代码块中执行
func (rf *Raft) getAppendEntriesArgs(server int) AppendEntriesArgs {
	preLogIndex := rf.nextIndex[server] - 1
	preLogTerm := rf.log[rf.toSliceIndex(preLogIndex)].Term
	var entries []Entry
	if i := rf.toSliceIndex(rf.nextIndex[server]); i < len(rf.log) {
		entries = append(entries, rf.log[i:]...)
	}
	return AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		preLogIndex,
		preLogTerm,
		entries,
		rf.commitIndex,
	}
}

func (rf *Raft) beginHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.sendHeartbeatAll()
		} else {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(rf.heartTime)
	}
}

// 需要在同步代码块中执行
func (rf *Raft) sendHeartbeatAll() {
	rf.updateNextIndex() // 使用nextIndex之前必须更新
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		if rf.nextIndex[idx] > rf.snapshotIndex || rf.snapshotIndex == 0 { // 发送日志
			args := rf.getAppendEntriesArgs(idx)
			DPrintf("节点 [%d] 给节点 [%d] 发送心跳, args = [%+v]", rf.me, idx, args)
			go rf.sendAppendEntriesOne(idx, args)
		} else { // 发送快照
			args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot}
			DPrintf("节点 [%d] 给节点 [%d] 发送快照, args = [%+v]", rf.me, idx, args)
			go rf.sendSnapshotOne(idx, args)
		}
	}
}

func (rf *Raft) sendAppendEntriesOne(idx int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(idx, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
	} else {
		if args.Term != rf.currentTerm || rf.state != LEADER {
			// 不处理跨term的心跳返回
			return
		}
		if !reply.Success {
			rf.retryNextIndex(idx, args, reply)
		} else {
			rf.matchIndex[idx] = Max(args.PreLogIndex+len(args.Entries), rf.matchIndex[idx]) // 在等待RPC返回期间，leader有可能添加了新日志; 落后的rpc返回的matchIndex可能比当前值小
			rf.nextIndex[idx] = rf.matchIndex[idx] + 1
			rf.commit()
		}
	}
}

// 需要在同步代码块中执行
func (rf *Raft) retryNextIndex(idx int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if reply.XLen <= args.PreLogIndex {
		// follower's log is too short
		rf.nextIndex[idx] = reply.XLen
	} else {
		findXTerm := false
		for i := rf.toSliceIndex(args.PreLogIndex); i > 0 && rf.log[i].Term >= reply.XTerm; i-- {
			if rf.log[i].Term == reply.XTerm {
				// leader has XTerm
				findXTerm = true
				rf.nextIndex[idx] = rf.toRealIndex(i)
				break
			}
		}
		if !findXTerm {
			// leader doesn't have XTerm
			rf.nextIndex[idx] = reply.XIndex
		}
	}
	rf.nextIndex[idx] = Max(rf.nextIndex[idx], reply.CommitIndex+1)
}

func (rf *Raft) commit() {
	// 二分查找新的commitIndex
	left, right := rf.commitIndex+1, rf.toRealIndex(len(rf.log))-1
	for right >= left {
		mid := (right + left) / 2
		cnt := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= mid {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	if rf.log[rf.toSliceIndex(right)].Term == rf.currentTerm && rf.state == LEADER { //只有当前任期内的entry可以通过计数提交，并将之前的entry一并提交(Figure 8)
		rf.commitIndex = right
		rf.applyCond.Broadcast()
	}
}

// AE rpc响应
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//DPrintf("节点 [%d] 接收节点 [%d] 心跳, args = [%+v]", rf.me, args.LeaderId, args)
	if args.Term >= rf.currentTerm {
		rf.updateHeartTime()
		rf.leaderId = args.LeaderId
		if i := rf.toSliceIndex(args.PreLogIndex); i < len(rf.log) && i >= 0 { // 要求i >= 0是由于，心跳在心跳发送和接收期间，follower可能制作了snapshot导致i < 0
			if rf.log[i].Term == args.PreLogTerm {
				reply.Success = true
				if len(args.Entries) != 0 || i != len(rf.log)-1 {
					// 避免落后rpc修改rf.log，而且落后的rpc的args.LeaderCommit可能小于rf.commitIndex，不进行判断就修改日志，可能会导致rf.commitIndex >= len(rf.log)
					if i+len(args.Entries) > len(rf.log)-1 || !EntriesEqual(rf.log[i+1:i+1+len(args.Entries)], args.Entries) {
						tmp := make([]Entry, i+1)
						copy(tmp, rf.log[:i+1])
						rf.log = append(tmp, args.Entries...)
					}
				}
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = Min(args.LeaderCommit, rf.toRealIndex(len(rf.log))-1)
					rf.applyCond.Broadcast()
				}
			} else {
				// 寻找最后entry的term内最早的entry索引，若未找到则取follower的snapshotIndex+1
				reply.XTerm = rf.log[i].Term
				k := i
				for k > 1 && rf.log[k].Term == rf.log[k-1].Term {
					k--
				}
				reply.XIndex = rf.toRealIndex(k)
			}

		}
		reply.XLen = rf.toRealIndex(len(rf.log))
		reply.CommitIndex = rf.commitIndex
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
