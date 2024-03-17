package raft

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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 持久化数据
	currentTerm   int
	votedFor      VoteFor
	log           []Entry
	snapshot      []byte // 快照
	snapshotTerm  int    // 快照包含的最后一条entry的term
	snapshotIndex int    // 快照包含的最后一条entry的Index

	commitIndex int
	lastApplied int

	// leader属性，选举完成后要重新初始化
	nextIndex  []int
	matchIndex []int

	state         State         // 节点角色
	lastHeartTime time.Time     // 最近一次心跳时间
	leaderId      int           // 用于重定向client请求
	heartTime     time.Duration // 心跳时间
	applyCh       chan ApplyMsg
	applyCond     *sync.Cond
	wg            sync.WaitGroup
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader := rf.currentTerm, rf.state == LEADER
	return term, isLeader
}

func (rf *Raft) GetLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	leaderId := rf.leaderId
	return leaderId
}

// 需要在同步代码块中执行
func (rf *Raft) persist() {
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

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, snapshotIndex, snapshotTerm int
	var voteFor VoteFor
	var lg []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&lg) != nil || d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		log.Printf("raft, 节点：[%d] 读取持久化数据失败\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = lg

		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
		rf.snapshot = rf.persister.ReadSnapshot() // 避免以下情况：service未向raft发送snapshot，persist()持久化时将为nil的rf.snapshot保持至持久层覆盖原来的snapshot

		// 重启节点的commitIndex和lastApplied必须与持久层读取的snapshotIndex一致，保证数据一致性
		rf.commitIndex = snapshotIndex
		rf.lastApplied = snapshotIndex
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	sliceIdx := rf.toSliceIndex(index)
	if sliceIdx > 0 {
		rf.snapshot = snapshot
		rf.snapshotTerm = rf.log[sliceIdx].Term
		rf.snapshotIndex = index
		rf.trimEntry(sliceIdx)
		rf.persist()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		return 0, 0, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.toRealIndex(len(rf.log))
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		rf.log = append(rf.log, Entry{
			Term:    rf.currentTerm,
			Command: command,
			Index:   index,
		})
		rf.persist()
		rf.sendHeartbeatAll()
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
	DPrintf("节点：[%d] 停止\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// 设置选举超时时间(750ms-1000ms)
		ms := 750 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != LEADER && time.Since(rf.lastHeartTime).Milliseconds() >= ms {
			rf.state = CANDIDATE
			rf.currentTerm++
			rf.votedFor = VoteFor{rf.currentTerm, rf.me}
			rf.persist()
			//DPrintf("节点 [%d] 开启选举", rf.me)
			//DPrintf("节点 [%d] state = %v term = %d", rf.me, rf.state, rf.currentTerm)
			rf.sendRequestVoteAll()
		}
		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.heartTime = time.Duration(110) * time.Millisecond
	rf.log = make([]Entry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.wg.Add(1)

	DPrintf("节点：[%d] 启动\n", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()

	go rf.DebugInfo()

	return rf
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		begin := rf.toSliceIndex(rf.lastApplied) + 1
		end := rf.toSliceIndex(rf.commitIndex) + 1
		entries := make([]Entry, end-begin)
		copy(entries, rf.log[begin:end])
		rf.mu.Unlock()

		for _, entry := range entries {
			if !rf.killed() {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.mu.Lock()
				rf.lastApplied = Max(rf.lastApplied, entry.Index) //安装快照会改变rf.lastApplied
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) DebugInfo() {
	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("节点 [%d], state = [%v], term = [%d], nextIndex = [%v], matchIndex = [%v], snapshotIndex = [%d] ,commitIndex = [%d], lastApplied = [%d], log = [%d]", rf.me, rf.state, rf.currentTerm, rf.nextIndex, rf.matchIndex, rf.snapshotIndex, rf.commitIndex, rf.lastApplied, rf.log)
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(10))
	}
}
