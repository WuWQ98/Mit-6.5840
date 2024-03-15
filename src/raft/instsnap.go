package raft

import (
	"time"
)

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshotOnce(idx int) {
	isArgs := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot}
	isReply := &InstallSnapshotReply{}
	go func() {
		DPrintf("%v ---- 节点：%d 给 %d 发送快照\n", time.Now(), rf.me, idx)
		ok := rf.sendInstallSnapshot(idx, isArgs, isReply)
		if !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if isReply.Term > rf.currentTerm {
			rf.updateTerm(isReply.Term)
		}
	}()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.wg.Add(1)
	defer rf.wg.Done()
	DPrintf("%v ---- 节点：%d 接收 %d 的快照\n", time.Now(), rf.me, args.LeaderId)
	if args.Term >= rf.currentTerm {
		rf.leaderId = args.LeaderId
		rf.updateTime()
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
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Snapshot,
				SnapshotTerm:  args.LastIncludeTerm,
				SnapshotIndex: args.LastIncludeIndex,
			}
			rf.snapshot = args.Snapshot
			rf.snapshotIndex = args.LastIncludeIndex
			rf.snapshotTerm = args.LastIncludeTerm

			rf.log = make([]Entry, 1)

			rf.log[0].Term = args.LastIncludeTerm
			rf.commitIndex = args.LastIncludeIndex
			rf.lastApplied = args.LastIncludeIndex
			rf.persist()
		}
	}
	reply.Term = rf.currentTerm
}
