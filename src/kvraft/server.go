package kvraft

import (
	"time"
	"../labgob"
	"../labrpc"
	"../raft"
	"sync"
	"sync/atomic"
)

//Result s
type Result struct {
	OpIndex int64
	Value   string
}

// Op s
type Op struct {
	Key      string
	Value    string
	Client int64
	OpType   string
	OpIndex  int64
}

//KVServer struct
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data                map[string]string
	callbackCh          map[int64]chan Result // [Client] result
	lastCommitedOpIndex map[int64]int64

	lastCommitedLogIndex int
}

//Get hendler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()

	if isLeader {
		reply.InvalidLeader = false
		kv.mu.Lock()
		if _, ok := kv.callbackCh[args.Client]; !ok {
			kv.callbackCh[args.Client] = make(chan Result)
		}
		kv.mu.Unlock()

		kv.rf.Start(Op{args.Key, "", args.Client, "Get", args.OpIndex})

		select {
		case res := <-kv.callbackCh[args.Client]:
			if res.OpIndex == args.OpIndex {
				if res.Value != "" {
					reply.Err = OK
					reply.Value = res.Value
				} else {
					reply.Err = ErrNoKey
				}
			}
		case <-time.After(2 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.InvalidLeader = true
	}

}

//PutAppend hendler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isLeader := kv.rf.GetState()

	if isLeader {
		reply.InvalidLeader = false
		kv.mu.Lock()
		if _, ok := kv.callbackCh[args.Client]; !ok {
			kv.callbackCh[args.Client] = make(chan Result)
		}
		kv.mu.Unlock()

		kv.rf.Start(Op{args.Key, args.Value, args.Client, args.Op, args.OpIndex})

		select {
		case res := <-kv.callbackCh[args.Client]:
			if res.OpIndex >= args.OpIndex {
				reply.Err = OK
			}
		case <-time.After(2 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.InvalidLeader = true
	}

}

//Kill a
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer f
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate


	kv.data = make(map[string]string)
	kv.callbackCh = make(map[int64]chan Result)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastCommitedOpIndex = make(map[int64]int64)

	go kv.loopForApplyMsg()
	return kv
}

//ApplyCommand h
func (kv *KVServer) ApplyCommand(command Op) Result {
	result := Result{command.OpIndex, ""}

	if kv.lastCommitedOpIndex[command.Client] < command.OpIndex {
		
		switch command.OpType {
		case "Put":
			kv.data[command.Key] = command.Value
		case "Append":
			kv.data[command.Key] += command.Value
		}
		
		kv.lastCommitedOpIndex[command.Client] = command.OpIndex
	}
	if val, ok := kv.data[command.Key]; ok {
		result.Value = val
	}
	return result
}


func (kv *KVServer) loopForApplyMsg() {
	for {
		msg := <-kv.applyCh
	
		command := msg.Command.(Op)
		res := kv.ApplyCommand(command)
		if msg.CommandIndex > kv.lastCommitedLogIndex {
			kv.lastCommitedLogIndex = msg.CommandIndex
		}
		select {
		case kv.callbackCh[command.Client] <- res:
		default:
		}

	}
}


