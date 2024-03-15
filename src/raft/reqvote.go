package raft

import (
	"sync"
)

// 需要在同步代码块中执行
func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	return RequestVoteArgs{rf.currentTerm, rf.me, rf.toLogIndex(len(rf.log)) - 1, rf.log[len(rf.log)-1].Term}
}

// 需要在同步代码块中执行
func (rf *Raft) checkCandidateLog(lastLogIndex, lastLogTerm int) (res bool) {
	myLastLogTerm := rf.log[len(rf.log)-1].Term
	if lastLogTerm == myLastLogTerm { // term相同，比较log长度
		res = lastLogIndex+1 >= rf.toLogIndex(len(rf.log))
	} else { // term不同，比较最后日志条目的term
		res = lastLogTerm > myLastLogTerm
	}
	return res
}

// 需要在同步代码块中执行
func (rf *Raft) sendRequestVoteOnce() {
	voteCnt := 1
	args := rf.getRequestVoteArgs() //同步获取参数
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
				// DPrintf("%v ---- 节点：%d 持久化数据：term = %d，voteFor = %v，log = %v (SendRV)\n", time.Now(), rf.me, rf.currentTerm, rf.votedFor, rf.log)
			} else {
				if reply.VoteGranted && rf.loadState() == CANDIDATE && args.Term == rf.currentTerm { // args.Term == rf.currentTerm 用来判断rpc请求返回是否跨任期，即是否过期
					voteCnt++
					if voteCnt > len(rf.peers)/2 { // 赢得选举
						once.Do(rf.beLeader)
					}
				}
			}
			// DPrintf("%v ---- 节点: %d 投票结果，结果 => 投票节点：%d, 当前得票数：%d, 当前角色：%d\n", time.Now(), rf.me, idx, voteCnt, rf.loadRole())
		}(idx)
	}

}

// 需要在同步代码块中执行
func (rf *Raft) beLeader() {
	rf.storeState(LEADER)
	// 初始化leader
	rf.initLeader()
	// 开始并行定期发送AE心跳
	go rf.startAppendEntriesLoop()
}

// 需要在同步代码块中执行
func (rf *Raft) initLeader() {
	for i := range rf.nextIndex {
		if i != rf.me {
			rf.nextIndex[i] = rf.toLogIndex(len(rf.log))
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
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		if rf.votedFor.Term == args.Term && rf.votedFor.CandidateId != args.CandidateId { // 相同任期内已经投票给其他candidate
			// DPrintf("%v ---- 节点：%d 收到 %d 投票请求，但同term内已投票，拒绝投票\n", time.Now(), rf.me, args.CandidateId)
			reply.VoteGranted = false
		} else {
			if rf.checkCandidateLog(args.LastLogIndex, args.LastLogTerm) { // candidate的log未过时
				// DPrintf("%v ---- 节点：%d 收到 %d 投票请求，完成投票\n", time.Now(), rf.me, args.CandidateId)
				// 重置超时选举时间放在这以避免如下情况：假设3节点系统（A、B、C）中，leader A 因为网络故障脱离系统，而节点 B 的日志相较于节点 C 过时，
				// 若节点 B 先开启选举，且重置超时时间不在此处，可能会使节点 B 会选举失败又开启选举，导致节点 C 无法开启选举，最终导致系统长期无 leader
				rf.updateTime()
				reply.VoteGranted = true
				rf.votedFor = VoteFor{args.Term, args.CandidateId}
			} else { // candidate的log过时
				// DPrintf("%v ---- 节点：%d 收到 %d 过时log的投票请求，拒绝投票\n", time.Now(), rf.me, args.CandidateId)
				reply.VoteGranted = false
			}
		}
		if rf.loadState() != CANDIDATE { //避免以下情况：网络故障的旧leader节点恢复，当前集群中因为无leader节点而正在选举，rv rpc的任期同步功能直接将故障恢复旧leader不经过选举变为新leader
			rf.storeState(FOLLOWER)
		}
		rf.currentTerm = args.Term
	} else { // candidate的任期低于当前节点
		// DPrintf("%v ---- 节点：%d 收到 %d 过期term的投票请求，拒绝投票\n", time.Now(), rf.me, args.CandidateId)
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.persist() // 持久化数据
	// DPrintf("%v ---- 节点：%d 持久化数据：term = %d，voteFor = %v，log = %v\n (RV)", time.Now(), rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
