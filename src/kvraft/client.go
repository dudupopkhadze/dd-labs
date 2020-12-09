package kvraft

import (
	"time"
	"../labrpc"
	"crypto/rand"
	"math/big"
)

//Clerk struct
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderId int
	Client     int64
	opIndex      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//MakeClerk f
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeaderId = 0
	ck.Client = nrand()
	ck.opIndex = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.opIndex++

	var result string
	leaderId := ck.lastLeaderId
	for {
		args := GetArgs{key, ck.Client, ck.opIndex}
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if ok && !reply.InvalidLeader {
			if reply.Err == OK {
				result = reply.Value
				break
			} else if reply.Err == ErrNoKey {
				result = ""
				break
			}
		}
		if reply.Err != TimeOut {
			leaderId = (leaderId + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
	ck.lastLeaderId = leaderId

	return result
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.opIndex++

	leaderId := ck.lastLeaderId
	for {
		args := PutAppendArgs{key, value, op, ck.Client, ck.opIndex}
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.InvalidLeader && reply.Err == OK {
			break
		}
		if reply.Err != TimeOut {
			leaderId = (leaderId + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
	ck.lastLeaderId = leaderId

	return
}

//Put hendler
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//Append hendler
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
