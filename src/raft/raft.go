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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// helper types

//CONSTS

const pingPerMilliseconds = 100 * time.Millisecond

const noPreferedCandidate = -1

//ServerStatus represents current status of the server
type ServerStatus string

//ServerStatus possibleValues
const (
	Leader    ServerStatus = "leader"
	Follower               = "follower"
	Candidate              = "candidate"
)

// AppendEntriesArgs s
type AppendEntriesArgs struct {
	Term int
	ID   int
}

// AppendEntriesReply s
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term   int
	Succes bool
}

// AppendEntries f
// currently used for pingign folowers
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.evaluateTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Succes = true
	reply.Term = rf.term
	if args.Term < rf.term {
		return
	}
	rf.lastValidHeartbeat = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ApplyMsg s
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Raft s
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	status             ServerStatus
	lastValidHeartbeat bool
	term               int
	preferedCandidate  int
}

//helper functions

func getRandomInRange(min int, max int) int {
	return min + rand.Intn(max-min)
}

func getRandomTimeoutDuration() time.Duration {
	return time.Duration(getRandomInRange(1000, 1500)) * time.Millisecond
}

func slepForAllowedDuration() {
	time.Sleep(getRandomTimeoutDuration())
}

// GetState f
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	isleader := rf.status == Leader
	return rf.term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// RequestVoteArgs s
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	ID   int
}

// RequestVoteReply s
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term     int
	Aprroved bool
}

// RequestVote f
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.evaluateTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.Aprroved = false
	if args.Term < rf.term || (rf.preferedCandidate != noPreferedCandidate && args.ID != rf.preferedCandidate) {
		return
	}

	reply.Aprroved = true
	rf.preferedCandidate = args.ID
	if rf.status == Follower {
		rf.lastValidHeartbeat = true
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
//analyzes given term
//if term is greates then our current term -> our  term/status are updated
func (rf *Raft) evaluateTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.term {
		rf.status = Follower
		rf.preferedCandidate = noPreferedCandidate
		rf.term = term
	}
}

func (rf *Raft) isEnoughVotes(votes int) bool {
	return votes >= (len(rf.peers)+1)/2
}

// counts election results
// returns true only if n+1/2 servers returned Approved=True
func (rf *Raft) isEnoughVotesForRevolution(channel *chan bool) bool {
	myElectorate := 1
	for i := 1; i < len(rf.peers); i++ {
		if <-*channel {
			myElectorate++
			if rf.isEnoughVotes(myElectorate) {
				return true
			}
		}
	}
	return false
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Candidate {
		return
	}
	rf.status = Leader
}

// requests vote for election from given server
func (rf *Raft) requestVoteFromServer(server int, channel *chan bool, request *RequestVoteArgs) {
	response := RequestVoteReply{-1, false}
	ok := rf.sendRequestVote(server, request, &response)
	rf.evaluateTerm(response.Term)
	select {
	case *channel <- ok && response.Aprroved: // voted
	case <-time.After(100 * time.Millisecond): // late
	}
}

// requests vote from every other server
//-> counts them
//-> becomes leader if can
func (rf *Raft) startElection() {
	request := RequestVoteArgs{rf.term, rf.me}
	channel := make(chan bool)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestVoteFromServer(i, &channel, &request)
	}

	if rf.isEnoughVotesForRevolution(&channel) {
		rf.becomeLeader()
	}
}

//cesco
func (rf *Raft) nominateSelfAsCandidate() {
	rf.status = Candidate
	rf.term++
	rf.preferedCandidate = rf.me
	go rf.startElection()
}

// nominates self as candidate
// only if status is not leader and has not been pinged in while
func (rf *Raft) observeElection() {
	for {
		slepForAllowedDuration()
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.status != Leader && !rf.lastValidHeartbeat {
				rf.nominateSelfAsCandidate()
			}
			rf.lastValidHeartbeat = false
		}()
	}
}

// is called only when current status is leader
// pings other servers -
// -> so they know King is alive
func (rf *Raft) pingServer(server int) {
	args := AppendEntriesArgs{rf.term, rf.me}
	res := AppendEntriesReply{-1, false}
	rf.sendAppendEntries(server, &args, &res)
	rf.evaluateTerm(res.Term)
}

// if leader => ping followers
func (rf *Raft) keepFollowersOnAlert() {
	for {
		if rf.status == Leader {
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.pingServer(i)
			}
		}
		time.Sleep(pingPerMilliseconds)
	}
}

// Make fun
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.lastValidHeartbeat = false
	rf.status = Follower
	rf.term = 0
	rf.preferedCandidate = noPreferedCandidate

	go rf.observeElection()
	go rf.keepFollowersOnAlert()
	rf.readPersist(persister.ReadRaftState())
	return rf
}
