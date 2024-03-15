package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type CommandArgs struct {
	Key   string
	Value string
	Op    string
	CkId  string
	SeqId int64
}

type CommandReply struct {
	Err   Err
	Value string
}
