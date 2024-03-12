package raft

import "time"

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot() {
	for server := range rf.peers {
		if server != rf.me {
			rf.sendSnapshot2One(server)
		}
	}
}

func (rf *Raft) sendSnapshot2One(idx int) {
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
		} else {
			rf.matchIndex[idx] = isArgs.LastIncludeIndex
			rf.nextIndex[idx] = rf.toLogIndex(len(rf.log))
		}
	}()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v ---- 节点：%d 接收 %d 的快照\n", time.Now(), rf.me, args.LeaderId)
	if args.Term >= rf.currentTerm {
		rf.leaderId = args.LeaderId
		if rf.snapshotIndex < args.LastIncludeIndex {

			ch := make(chan bool)

			go func() {
				rf.applyCh <- ApplyMsg{
					SnapshotValid: true,
					Snapshot:      args.Snapshot,
					SnapshotTerm:  args.LastIncludeTerm,
					SnapshotIndex: args.LastIncludeIndex,
				}
				ch <- true
			}()

			select {
			case <-ch:
				rf.snapshot = args.Snapshot
				rf.snapshotIndex = args.LastIncludeIndex
				rf.snapshotTerm = args.LastIncludeTerm

				rf.log = make([]LogInfo, 1)
				rf.log[0].Term = args.LastIncludeTerm
				rf.commitIndex = args.LastIncludeIndex
				rf.lastApplied = args.LastIncludeIndex
			case <-time.NewTimer(time.Millisecond * time.Duration(1500)).C:
				if rf.killed() {
					close(rf.applyCh)
				}
			}
		}

		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm
}
