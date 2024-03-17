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
		// rf.commitIndex 可能大于 args.LastIncludeIndex
		// 假设leader A向follower B发送心跳rpc0，由于B日志落后过多，日志匹配不成功
		// rpc0返回后，A给B发送一个快照rpc1
		// 接着，A向B发送心跳rpc2，rpc2执行时，rpc1已经未执行，日志匹配不成功（由于心跳间隔极短，rpc2比rpc1更快到达，亦或者rpc2比rpc1更先发送）
		// 接着，A向B发送心跳rpc3，rpc3执行时，rpc1已经执行完毕，此时rpc3会得到日志匹配成功的结果，然后将B的commitIndex设置为A的CommitIndex或B的lastEntry的Index
		// 接着，rpc2返回后A发送的一个LastIncludeIndex更大的快照rpc4到达B（假设这段时间内A又制作了新的快照，取决kvserver判断是否制作快照的方式）
		// rpc4中rf.commitIndex 就有可能大于 args.LastIncludeIndex，只要A的CommitIndex或B的lastEntry的Index或大于args.LastIncludeIndex
		// ps:接收entry时发送心跳导致心跳间隔可以极短

		// 如果在rf.commitIndex > args.LastIncludeIndex的情况下安装快照，可能有index > args.LastIncludeIndex的entry在applyCh上，
		// 该entry应用之后就会导致lastApplied = index > snapshotIndex = args.LastIncludeIndex,然后如果kvserver制作快照就会使得snapshotIndex = index > commitIndex = args.LastIncludeIndex
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
