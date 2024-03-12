package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久化数据
	currentTerm   int
	votedFor      VoteForInfo
	log           []LogInfo
	snapshot      []byte // 快照
	snapshotTerm  int    // 快照包含的最后一条entry的term
	snapshotIndex int    // 快照包含的最后一条entry的Index

	commitIndex int
	lastApplied int

	// leader属性，选举完成后要重新初始化
	nextIndex  []int
	matchIndex []int

	myRole        role          // 节点角色
	lastHeartTime time.Time     // 最近一次心跳时间
	leaderId      int           // 用于重定向client请求
	heartTime     time.Duration // 心跳时间
	applyCh       chan ApplyMsg

	// commitIndex >= lastApplied >= snapshotIndex
}

type role int32

const (
	FOLLOWER role = iota
	CANDIDATE
	LEADER
)

type LogInfo struct {
	Term    int
	Command interface{}
}

type VoteForInfo struct {
	Term        int
	CandidateId int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader := rf.currentTerm, rf.loadRole() == LEADER
	return term, isLeader
}

func (rf *Raft) GetLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	leaderId := rf.leaderId
	return leaderId
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// 需要在同步代码块中执行
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
// 节点启动时执行，不需要加锁
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, snapshotIndex, snapshotTerm int
	var voteFor VoteForInfo
	var log []LogInfo
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil || d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		DPrintf("%v ---- 节点：%d 读取持久化数据失败\n", time.Now(), rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log

		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
		rf.snapshot = rf.persister.ReadSnapshot() // 避免以下情况：service未向raft发送snapshot，persist()持久化时将为nil的rf.snapshot保持至持久层覆盖原来的snapshot

		// 重启节点的commitIndex和lastApplied必须与持久层读取的snapshotIndex一致，保证数据一致性
		rf.commitIndex = snapshotIndex
		rf.lastApplied = snapshotIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go func() {
		rf.mu.Lock()
		sliceIdx := rf.toSliceIndex(index)
		// DPrintf("%v ---- 节点: %d 接收server快照，index = %d, rf.snapshotIndex = %d, sliceIdx = %d, len(rf.log) = %d\n", time.Now(), rf.me, index, rf.snapshotIndex, sliceIdx, len(rf.log))
		if sliceIdx > 0 && sliceIdx < len(rf.log) {
			rf.snapshot = snapshot
			rf.snapshotTerm = rf.log[sliceIdx].Term
			rf.snapshotIndex = index
			rf.trimLog(sliceIdx)
			rf.log[0].Term = rf.snapshotTerm // log[0]为占位entry，方便 AE rpc 和 RV rpc 的term比较
			rf.persist()
		}
		rf.mu.Unlock()
	}()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.toLogIndex(len(rf.log))
	term := rf.currentTerm
	isLeader := rf.loadRole() == LEADER

	if isLeader {
		DPrintf("%v ---- 节点：%d 接收日志条目 entry = %+v\n", time.Now(), rf.me, LogInfo{Term: rf.currentTerm, Command: command})
		rf.log = append(rf.log, LogInfo{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.persist()
		rf.sendAppendEntriesOnce()
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	DPrintf("%v ---- 节点：%d 停止\n", time.Now(), rf.me)
	// DPrintf("%v ---- 节点：%d 停止时快照为：%v\n", time.Now(), rf.me, rf.snapshot)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 500 and 800 milliseconds.
		// 设置选举超时时间(750ms-1000ms)
		ms := 750 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.loadRole() != LEADER && time.Since(rf.lastHeartTime).Milliseconds() >= ms {
			rf.storeRole(CANDIDATE)

			DPrintf("%v ---- 节点：%d 开启选举\n", time.Now(), rf.me)

			rf.currentTerm++                                 // 增加任期
			rf.votedFor = VoteForInfo{rf.currentTerm, rf.me} // 给自己投票
			rf.persist()                                     // 持久化数据

			rf.sendRequestVoteOnce()
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// DPrintf("%v ---- 节点：%d 启动\n", time.Now(), rf.me)
	rf.applyCh = applyCh
	rf.heartTime = time.Duration(110) * time.Millisecond
	rf.log = make([]LogInfo, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commit()

	go rf.applyLog()

	go rf.printRaftInfo()

	return rf
}

func (rf *Raft) applyLog() {
	time.Sleep(time.Duration(1000) * time.Millisecond)
	for !rf.killed() {
		rf.mu.Lock()
		begin := rf.toSliceIndex(rf.lastApplied) + 1
		end := rf.toSliceIndex(rf.commitIndex) + 1
		if begin < len(rf.log) && begin < end {
			for i, log := range rf.log[begin:end] {
				if !rf.killed() {
					ch := make(chan bool)
					go func(i int, log LogInfo) {
						defer func() {
							if r := recover(); r != nil {
							}
						}()
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      log.Command,
							CommandIndex: rf.toLogIndex(begin) + i,
						}
						ch <- true
					}(i, log)
					select {
					case <-ch:
						rf.lastApplied = rf.toLogIndex(begin) + i
					case <-time.NewTimer(time.Millisecond * time.Duration(1500)).C:
						if rf.killed() {
							close(rf.applyCh)
						}
					}
				} else {
					break
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) commit() {
	time.Sleep(time.Duration(1000) * time.Millisecond)
	for !rf.killed() {
		rf.mu.Lock()
		// 二分查找新的commitIndex
		if rf.commitIndex < rf.snapshotIndex {
			log.Fatalln("致命错误 commitIndex < snapshotIndex", "commitIndex = ", rf.commitIndex, "snapshotIndex = ", rf.snapshotIndex)
		}
		left, right := rf.commitIndex+1, rf.toLogIndex(len(rf.log))-1
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
		if rf.log[rf.toSliceIndex(right)].Term == rf.currentTerm { //只有当前任期内的entry可以通过计数提交，并将之前的entry一并提交(Figure 8)
			rf.commitIndex = right
			// DPrintf("%v ---- 节点：%d commitIndex设置为 %d，触发节点为：%d\n", time.Now(), rf.me, rf.commitIndex, triggerIdx)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
}

func (rf *Raft) loadRole() role {
	return role(atomic.LoadInt32((*int32)(&rf.myRole)))
}

func (rf *Raft) storeRole(r role) {
	atomic.StoreInt32((*int32)(&rf.myRole), int32(r))
}

// 需要在同步代码块中执行
func (rf *Raft) updateTerm(term int) {
	rf.storeRole(FOLLOWER)
	rf.currentTerm = term
	rf.persist() // 持久化数据
}

// 需要在同步代码块中执行
func (rf *Raft) trimLog(sliceIdx int) {
	tmp := make([]LogInfo, 1, max(len(rf.log)-sliceIdx, 1))
	if len(rf.log)-sliceIdx > 1 {
		tmp = append(tmp, rf.log[sliceIdx+1:]...)
	}
	rf.log = tmp
}

func (rf *Raft) toSliceIndex(logIndex int) int {
	return logIndex - rf.snapshotIndex
}

func (rf *Raft) toLogIndex(sliceIndex int) int {
	return sliceIndex + rf.snapshotIndex
}

func (rf *Raft) printRaftInfo() {
	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("%v ---- 节点: %d, 任期: %d, role: %d, commitIndex: %d, lastApplied: %d, nextIndex: %v, matchIndex: %v, snapshotIndex: %v, snapshotTerm: %v, 日志长度: %d, 日志：%+v\n", time.Now(), rf.me, rf.currentTerm, rf.loadRole(), rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, rf.snapshotIndex, rf.snapshotTerm, len(rf.log), rf.log)
		rf.mu.Unlock()
		time.Sleep(time.Duration(25) * time.Millisecond)
	}
}
