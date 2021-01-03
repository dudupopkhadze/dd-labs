package kvraft

import (
	"encoding/gob"
	"bytes"
	"time"
	"../labgob"
	"../labrpc"
	"../raft"
	"sync"
	"sync/atomic"
)

//OPRes s
type OPRes struct {
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
	lock      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	maxraftstate int // snapshot if log grows this big

	data                map[string]string
	cb          map[int64]chan OPRes 
	lastOperation map[int64]int64
	lastLog int
}


func(kv *KVServer) startRaftForGet(args *GetArgs, reply *GetReply){
	reply.InvalidLeader = false
	kv.lock.Lock()
	
	if _, ok := kv.cb[args.Client]; !ok {
		kv.cb[args.Client] = make(chan OPRes)
	}

	kv.lock.Unlock()
	kv.rf.Start(Op{args.Key, "", args.Client, "Get", args.OpIndex})
}

//Get hendler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	
	if isLeader {
		kv.startRaftForGet(args,reply)

		select {
		case res := <-kv.cb[args.Client]:
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

func (kv * KVServer) startRaftForPutAppend(args *PutAppendArgs, reply *PutAppendReply){
	reply.InvalidLeader = false
	kv.lock.Lock()
	
	if _, ok := kv.cb[args.Client]; !ok {
		kv.cb[args.Client] = make(chan OPRes)
	}

	kv.lock.Unlock()
	kv.rf.Start(Op{args.Key, args.Value, args.Client, args.Op, args.OpIndex})
}


//PutAppend hendler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isLeader := kv.rf.GetState()

	if isLeader {
		kv.startRaftForPutAppend(args,reply)

		select {
		case res := <-kv.cb[args.Client]:
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

func (kv *KVServer) init(servers []*labrpc.ClientEnd, me int, persister *raft.Persister){
	kv.data = make(map[string]string)
	kv.cb = make(map[int64]chan OPRes)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastOperation = make(map[int64]int64)
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

	kv.init(servers,me,persister)

	go kv.applyMessagesLoop()
	return kv
}

func (kv *KVServer) applyMessagesLoop() {
	for {
		msg := <-kv.applyCh
		if msg.UseSnapshot {
			data := msg.Snapshot
			kv.deserializeSnapshot(data)
			kv.lastLog = msg.CommandIndex
		} else {
			command := msg.Command.(Op)
			res := kv.ApplyCommand(command)

			if msg.CommandIndex > kv.lastLog {
				kv.lastLog = msg.CommandIndex
			}
			select {
			case kv.cb[command.Client] <- res:
			default:
			}

			if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
				data := kv.serializeSnapshot()
				go kv.rf.UpdateSnapshot(data, kv.lastLog)
			}
		}

	}
}

func (kv * KVServer) updateDataByCommand(command Op){
	switch command.OpType {
	case "Put":
		kv.data[command.Key] = command.Value
	case "Append":
		kv.data[command.Key] += command.Value
	}
	kv.lastOperation[command.Client] = command.OpIndex
}

//ApplyCommand h
func (kv *KVServer) ApplyCommand(command Op) OPRes {
	result := OPRes{command.OpIndex, ""}

	if kv.lastOperation[command.Client] < command.OpIndex {
		kv.updateDataByCommand(command)
	}

	if val, ok := kv.data[command.Key]; ok {
		result.Value = val
	}
	return result
}

func (kv *KVServer) serializeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastOperation)
	return w.Bytes()
}

func (kv *KVServer) deserializeSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.lastOperation)
}


