package raft

import (
	"slices"
	"time"
)

// 需要在同步代码块中执行
func (rf *Raft) getAppendEntriesArgs(server int) AppendEntriesArgs {
	// 制作snapshot的时间是随机的，制作snapshot后，nextIndex-snapshotIndex 有可能小于等于0
	// 或者AE日志匹配失败后，计算的 nextIndex-snapshotIndex 有可能小于等于0
	// 若 nextIndex-snapshotIndex <= 0，则取nextIndex = snapshotIndex+1
	rf.nextIndex[server] = max(rf.nextIndex[server], rf.snapshotIndex+1)

	preLogIndex := rf.nextIndex[server] - 1
	preLogTerm := rf.log[rf.toSliceIndex(preLogIndex)].Term
	var entries []LogInfo
	if i := rf.toSliceIndex(rf.nextIndex[server]); i < len(rf.log) {
		entries = rf.log[i:]
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

func (rf *Raft) startAppendEntriesLoop() {
	for !rf.killed() && rf.loadRole() == LEADER {
		rf.mu.Lock()
		if rf.loadRole() == LEADER { // 双重判断是因为脱离网络的旧leader回归网络，获取该锁之前接收了新leader的心跳同步了term，此时若往外发送心跳就有可能错误修改follower日志
			DPrintf("%v ---- 节点：%d 发送心跳\n", time.Now(), rf.me)
			rf.sendAppendEntriesOnce()
		}
		rf.mu.Unlock()
		time.Sleep(rf.heartTime)
	}
}

// 需要在同步代码块中执行
func (rf *Raft) sendAppendEntriesOnce() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		args := rf.getAppendEntriesArgs(idx) // 同步获取AE参数
		go rf.AEResponse(idx, args)
	}
}

func (rf *Raft) AEResponse(idx int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(idx, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%v ---- 节点：%d 接收心跳返回值，发送term：%d，返回term：%d，日志匹配结果：%v，当前term：%d，当前角色：%d\n", time.Now(), rf.me, args.Term, reply.Term, reply.Success, rf.currentTerm, rf.loadRole())
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		// DPrintf("%v ---- 节点：%d 持久化数据：term = %d，voteFor = %v，log = %v\n (SendAE)", time.Now(), rf.me, rf.currentTerm, rf.votedFor, rf.log)
	} else {
		if args.Term != rf.currentTerm || rf.loadRole() != LEADER {
			// 不处理跨term的心跳返回
			return
		}
		if !reply.Success {
			if !(args.PreLogIndex == rf.snapshotIndex && rf.snapshotIndex != 0) { // 无需发送snapshot
				rf.retryNextIndex(idx, args, reply)
			} else { // 需要发送snapshot
				rf.sendSnapshot2One(idx)
			}
		} else { // 匹配成功
			DPrintf("%v ---- 节点 %d 对 节点 %d 日志匹配成功，preIndex = %d, entries = %v, matchIndex = %d\n", time.Now(), rf.me, idx, args.PreLogIndex, args.Entries, rf.matchIndex[idx])
			rf.matchIndex[idx] = max(args.PreLogIndex+len(args.Entries), rf.matchIndex[idx]) // 在等待RPC返回期间，leader有可能添加了新日志; 落后的rpc返回的matchIndex可能比当前值小
			rf.nextIndex[idx] = rf.matchIndex[idx] + 1
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
				rf.nextIndex[idx] = rf.toLogIndex(i)
				break
			}
		}
		if !findXTerm {
			// leader doesn't have XTerm
			rf.nextIndex[idx] = reply.XIndex
		}
	}
	rf.nextIndex[idx] = max(rf.nextIndex[idx], reply.CommitIndex+1)
}

// AE rpc响应
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v ---- 节点：%d 接收 %d 的心跳, entries = %v\n", time.Now(), rf.me, args.LeaderId, args.Entries)
	if args.Term >= rf.currentTerm {
		if t := time.Now(); t.After(rf.lastHeartTime) {
			// 重置超时选举时间
			rf.lastHeartTime = t
		}
		rf.leaderId = args.LeaderId
		if i := rf.toSliceIndex(args.PreLogIndex); i < len(rf.log) && i >= 0 { // 要求i >= 0是由于，心跳在心跳发送和接收期间，follower可能制作了snapshot导致i < 0
			if rf.log[i].Term == args.PreLogTerm { // 日志匹配
				reply.Success = true
				if len(args.Entries) != 0 || i != len(rf.log)-1 {
					// 避免落后rpc修改rf.log，而且落后的rpc的args.LeaderCommit可能小于rf.commitIndex，不进行判断就修改日志，可能会导致rf.commitIndex >= len(rf.log)
					if i+len(args.Entries) > len(rf.log)-1 || !slices.Equal(rf.log[i+1:i+1+len(args.Entries)], args.Entries) {
						tmp := make([]LogInfo, i+1, i+1+len(args.Entries))
						copy(tmp, rf.log[:i+1])
						rf.log = append(tmp, args.Entries...)
					}
				}
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = min(args.LeaderCommit, rf.toLogIndex(len(rf.log))-1)
				}
			} else {
				// 寻找最后entry的term内最早的entry索引，若未找到则取follower的snapshotIndex+1
				reply.XTerm = rf.log[i].Term
				k := i
				for k > 1 && rf.log[k].Term == rf.log[k-1].Term {
					k--
				}
				reply.XIndex = rf.toLogIndex(k)
			}

		}
		reply.XLen = rf.toLogIndex(len(rf.log))
		reply.CommitIndex = rf.commitIndex
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	// DPrintf("%v ---- 节点：%d 持久化数据：term = %d，voteFor = %v，log = %v\n (AE)", time.Now(), rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
