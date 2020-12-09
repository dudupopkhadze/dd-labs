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
	lastLeader int
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
	ck.lastLeader = 0
	ck.Client = nrand()
	ck.opIndex = 0
	return ck
}

func (ck * Clerk) getLoopHandler(key string, leader * int, res * string){
	for {
		args := GetArgs{key, ck.Client, ck.opIndex}
		reply := GetReply{}
		ok := ck.servers[*leader].Call("KVServer.Get", &args, &reply)
		if ok && !reply.InvalidLeader {
			if reply.Err == OK {
				*res = reply.Value
				break
			} else if reply.Err == ErrNoKey {
				*res = ""
				break
			}
		}
		if reply.Err != TimeOut {
			*leader = (*leader + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	res := ""
	leader := ck.lastLeader

	ck.getLoopHandler(key, &leader, &res)
	
	ck.lastLeader = leader
	return res
}


func (ck *Clerk) putAndAppendLoopHandler(key string, value string, op string, leader * int){
	for {
		args := PutAppendArgs{key, value, op, ck.Client, ck.opIndex}
		reply := PutAppendReply{}
		ok := ck.servers[*leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.InvalidLeader && reply.Err == OK {
			break
		}
		if reply.Err != TimeOut {
			*leader = (*leader + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	leader := ck.lastLeader

	ck.putAndAppendLoopHandler(key, value, op, &leader)

	ck.lastLeader = leader
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
