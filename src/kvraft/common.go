package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut  = "TimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Client int64
	OpIndex  int64
}

type PutAppendReply struct {
	InvalidLeader bool
	Err Err
}

type GetArgs struct {
	Key string
	Client int64
	OpIndex  int64
}

type GetReply struct {
	InvalidLeader bool
	Err   Err
	Value string
}
