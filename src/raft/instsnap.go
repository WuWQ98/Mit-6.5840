package raft

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%v ---- 节点：%d 接收 %d 的快照\n", time.Now(), rf.me, args.LeaderId)
	if args.Term >= rf.currentTerm {
		rf.leaderId = args.LeaderId
		if !(rf.snapshotIndex == args.LastIncludeIndex && rf.snapshotTerm == args.LastIncludeTerm) {

			rf.snapshot = args.Snapshot
			rf.snapshotIndex = args.LastIncludeIndex
			rf.snapshotTerm = args.LastIncludeTerm

			rf.lastApplied = args.LastIncludeIndex
			rf.commitIndex = args.LastIncludeIndex

			go func(snapshot []byte, snapshotTerm, snapshotIndex int) {
				rf.applyCh <- ApplyMsg{
					SnapshotValid: true,
					Snapshot:      snapshot,
					SnapshotTerm:  snapshotTerm,
					SnapshotIndex: snapshotIndex,
				}
			}(rf.snapshot, rf.snapshotTerm, rf.snapshotIndex)

			rf.log = make([]LogInfo, 1)
			rf.log[0].Term = rf.snapshotTerm
		}
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm
}
