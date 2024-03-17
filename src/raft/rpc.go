package raft

type State int32

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

type VoteFor struct {
	Term        int
	CandidateId int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	XIndex      int
	XLen        int
	XTerm       int
	CommitIndex int // 用来保证nextIndex大于follower的commitIndex
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
}

type InstallSnapshotReply struct {
	Term int
}
