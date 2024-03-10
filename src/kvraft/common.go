package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CkName    string
	Timestamp int64
}

type PutAppendReply struct {
	Retry bool
	Err   Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CkName    string
	Timestamp int64
}

type GetReply struct {
	Retry bool
	Err   Err
	Value string
}
