package raft

import (
	"sync"
)

// 需要在同步代码块中执行
func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	return RequestVoteArgs{rf.currentTerm, rf.me, rf.toRealIndex(len(rf.log)) - 1, rf.log[len(rf.log)-1].Term}
}

// 需要在同步代码块中执行
func (rf *Raft) validCandidate(lastLogIndex, lastLogTerm int) (res bool) {
	myLastLogTerm := rf.log[len(rf.log)-1].Term
	if lastLogTerm == myLastLogTerm {
		res = lastLogIndex+1 >= rf.toRealIndex(len(rf.log))
	} else {
		res = lastLogTerm > myLastLogTerm
	}
	return res
}

// 需要在同步代码块中执行
func (rf *Raft) sendRequestVoteAll() {
	voteCnt := 1
	args := rf.getRequestVoteArgs()
	var once sync.Once
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(idx, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.updateTerm(reply.Term)
			} else {
				if reply.VoteGranted && rf.state == CANDIDATE && args.Term == rf.currentTerm {
					voteCnt++
					if voteCnt > len(rf.peers)/2 {
						once.Do(rf.beLeader)
					}
				}
			}
		}(idx)
	}
}

// 需要在同步代码块中执行
func (rf *Raft) beLeader() {
	rf.state = LEADER
	rf.initLeader()
	go rf.beginHeartbeat()
}

// 需要在同步代码块中执行
func (rf *Raft) initLeader() {
	for i := range rf.nextIndex {
		if i != rf.me {
			rf.nextIndex[i] = rf.toRealIndex(len(rf.log))
		}
	}
	for i := range rf.matchIndex {
		if i != rf.me {
			rf.matchIndex[i] = 0
		}
	}

	rf.leaderId = rf.me
}

// RV rpc响应
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//DPrintf("节点 [%d] 接到投票请求，args = [%+v]", rf.me, args)
	//DPrintf("节点 [%d] state = [%v] term = [%d] voteFor = [%+v]", rf.me, rf.state, rf.currentTerm, rf.votedFor)
	if args.Term >= rf.currentTerm {
		if !(rf.votedFor.Term == args.Term && rf.votedFor.CandidateId != args.CandidateId) { // 同一term内未投给其他节点
			if rf.validCandidate(args.LastLogIndex, args.LastLogTerm) { // candidate的log未过时
				// 重置超时选举时间放在这以避免如下情况：假设3节点系统（A、B、C）中，leader A 因为网络故障脱离系统，而节点 B 的日志相较于节点 C 过时，
				// 若节点 B 先开启选举，且重置超时时间不在此处，可能会使节点 B 会选举失败又开启选举，导致节点 C 无法开启选举，最终导致系统长期无 leader
				rf.updateHeartTime()
				reply.VoteGranted = true
				rf.votedFor = VoteFor{args.Term, args.CandidateId}
			}
		}
		if rf.state != CANDIDATE { //避免以下情况：网络故障的旧leader节点恢复，当前集群中因为无leader节点而正在选举，rv rpc的任期同步功能直接将故障恢复旧leader不经过选举变为新leader
			rf.state = FOLLOWER
		}
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
