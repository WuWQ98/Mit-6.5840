package raft

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshotOne(idx int, args InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(idx, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	} else {
		rf.matchIndex[idx] = Max(rf.matchIndex[idx], args.LastIncludeIndex)
		rf.nextIndex[idx] = args.LastIncludeIndex + 1
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("节点 [%d] 接收节点 [%d] 快照, args = [%+v]", rf.me, args.LeaderId, args)
	if args.Term >= rf.currentTerm {
		rf.leaderId = args.LeaderId
		rf.updateHeartTime()
		rf.updateTerm(args.Term)
		// rf.commitIndex 可能大于 args.LastIncludeIndex, 原因如下：
		// 0. 接收 entry 时发送心跳导致心跳间隔可以变得极短
		// 1. 假设 leader A 向 follower B 发送心跳 rpc0，由于 B 日志落后过多，日志匹配不成功
		// 3. （ rpc0 返回后）A 向 B 发送快照 rpc1
		// 3. （ rpc0 未返回）A 向 B 发送快照 rpc2
		// 4. （ rpc1 返回后）A 向 B 发送心跳 rpc3
		// 5. 若 rpc3 比 rpc2 先到达（或者先获取锁），rpc3 日志匹配成功，修改 B 的日志，将 B 的 commitIndex 设置为 min(leaderCommit,lastIndex)
		// 6. rpc2 执行时就有可能有 LastIncludeIndex < commitIndex 的情况

		// 如果在 rf.commitIndex > args.LastIncludeIndex 的情况下安装快照，可能有 index > args.LastIncludeIndex 的 entry 在 applyCh 上，
		// 该 entry 应用之后就会导致 lastApplied = index > snapshotIndex = args.LastIncludeIndex, 然后如果 kvserver 制作快照就会使得 snapshotIndex = index > commitIndex = args.LastIncludeIndex
		if args.LastIncludeIndex > rf.snapshotIndex && args.LastIncludeIndex > rf.commitIndex { //避免过时快照
			go func(snapshot []byte, lastIncludeTerm, lastIncludeIndex int) {
				rf.applyCh <- ApplyMsg{
					SnapshotValid: true,
					Snapshot:      snapshot,
					SnapshotTerm:  lastIncludeTerm,
					SnapshotIndex: lastIncludeIndex,
				}
			}(args.Snapshot, args.LastIncludeTerm, args.LastIncludeIndex)

			i := rf.toSliceIndex(args.LastIncludeIndex)

			rf.snapshot = args.Snapshot
			rf.snapshotIndex = args.LastIncludeIndex
			rf.snapshotTerm = args.LastIncludeTerm
			rf.lastApplied = args.LastIncludeIndex
			rf.commitIndex = args.LastIncludeIndex

			rf.trimEntry(i)
			rf.persist()
		}
	}
	reply.Term = rf.currentTerm
}
