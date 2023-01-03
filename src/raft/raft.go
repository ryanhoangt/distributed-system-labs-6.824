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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
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

//
// A Go object implementing a single Raft peer.
//
type State int

const (
	Follower State = iota
	Candidate
	Leader

	ELECTION_TIMEOUT_LOWER int = 300
	ELECTION_TIMEOUT_UPPER int = 500
	HEARTBEAT_INTERVAL int = 120
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states
	currentTerm int
	votedFor int // index in peers[]
	logs []*LogEntry

	// Volatile states on all servers
	// commitIndex
	// lastApplied

	// Volatile states on leaders
	// nextIndex[]
	// matchIndex[]

	// Other auxiliary states
	curState State
	totalVotesReceived int
	heartbeatCh chan bool
	grantVoteCh chan bool
	stepDownCh chan bool
	winElecCh chan bool
}

func (rf *Raft) ToStr() string {
	return fmt.Sprintf("[rf %d term%d state%d]", rf.me, rf.currentTerm, rf.curState)
}

type LogEntry struct {
	Term int
	
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.curState == Leader

	return term, isleader
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Candidate int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[rf %d term%d state%d] receive RequestVote: %v\n", rf.me, rf.currentTerm, rf.curState, *args)

	// check stale request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// step down when having lower term
	if args.Term > rf.currentTerm { // current state can be any of 3 states
		if rf.curState == Follower {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		} else {
			fmt.Println("send step down signal inside RequestVote")
			rf.stepDownToFollower(args.Term)
		}
	}

	// reply back
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor < 0 || rf.votedFor == args.Candidate) {
	// && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.Candidate
		reply.VoteGranted = true
		rf.grantVoteCh <- true
	}
	
	fmt.Printf("[rf %d term%d state%d] reply to req %v: %v\n", rf.me, rf.currentTerm, rf.curState, *args, *reply)
}

func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	if lastLogTerm == rf.currentTerm {
		return lastLogIndex >= len(rf.logs) - 1
	}

	return lastLogTerm > rf.currentTerm
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	// PrevLogTerm int
	// PrevLogIndex int
	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[rf %d term%d state%d] receive heartbeat: %v\n", rf.me, rf.currentTerm, rf.curState, *args)

	// check stale request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// legitimate leader's heartbeat/append request
	if args.Term >= rf.currentTerm {
		if rf.curState == Follower {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.heartbeatCh <- true
		} else {
			fmt.Println("send step down signal inside AppendEntries")
			rf.stepDownToFollower(args.Term)
		}
	}

	// TODO:

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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.winElecCh = make(chan bool)
	rf.totalVotesReceived = 0
	rf.curState = Follower

	go rf.backgroundTask()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) backgroundTask()  {
	for (!rf.killed()) {
		rf.mu.Lock()
		curState := rf.curState
		rf.mu.Unlock()

		switch curState {
		case Follower:
			select {
			case <- rf.heartbeatCh:
			case <- rf.grantVoteCh:
			case <- time.After(time.Millisecond * getElectionTimeout()):
				rf.convertToCandidate()
			}
		case Candidate:
			select {
			case <- rf.stepDownCh:
				// state should already be follower
			case <- rf.winElecCh:
				rf.convertToLeader()
			case <- time.After(time.Millisecond * getElectionTimeout()):
				fmt.Printf("[rf %d] cannot become leader for term %d, start new election\n", rf.me, rf.currentTerm)
				rf.convertToCandidate()
			}
		case Leader:
			select {
			case <- rf.stepDownCh:
				fmt.Printf("[rf %d term%d state%d] step down from leader to follower\n", rf.me, rf.currentTerm, rf.curState)
				// state should already be follower
			case <- time.After(time.Millisecond * time.Duration(HEARTBEAT_INTERVAL)):
				rf.mu.Lock()
				rf.broadcastHeartbeat()
				rf.mu.Unlock()
			}
			
		}
	}

}

func (rf* Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[rf %d] becoming leader for term %d\n", rf.me, rf.currentTerm)
	
	// rf.resetAllChannels()
	rf.curState = Leader

	// send heartbeats to establish authority
	rf.broadcastHeartbeat()
}

// only call when lock is acquired
func (rf *Raft) broadcastHeartbeat() {
	fmt.Printf("[rf %d term%d state%d] broadcasting heartbeat\n", rf.me, rf.currentTerm, rf.curState)

	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		Entries: []LogEntry{},
	}

	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}
		go func (serverId int)  {
			reply := AppendEntriesReply{}
			succ := rf.sendAppendEntries(serverId, &args, &reply)

			if !succ {
				// log.Fatalf("cannot send heartbeat to peer %d\n", serverId)
				return
			}

		}(serverId)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf.resetAllChannels()
	rf.curState = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.totalVotesReceived = 1

	rf.broadcastRequestVote()
}

func (rf *Raft) broadcastRequestVote() {
	args := RequestVoteArgs{
		Term: rf.currentTerm, 
		Candidate: rf.me,
		LastLogTerm: rf.currentTerm,
		LastLogIndex: len(rf.logs) - 1,
	}

	for serverId := range rf.peers {
		// issue RequestVote in parallel to each of the other servers
		if serverId == rf.me {
			continue
		}
		go func (serverId int) {	
			reply := RequestVoteReply{}
			succ := rf.sendRequestVote(serverId, &args, &reply)

			if !succ {
				// log.Fatalf("cannot send RequestVote to peer %d\n", serverId)
				return
			}
			
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// check state again after time-consuming RPC
			if rf.curState != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
				return
			}

			if reply.Term > rf.currentTerm {
				fmt.Println("send step down signal inside broadcastReqVote")
				rf.stepDownToFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				rf.totalVotesReceived += 1

				if rf.totalVotesReceived == len(rf.peers) / 2 + 1 {
					rf.winElecCh <- true
				}
			}

			
		}(serverId)
	}
}

// lock must be acquired when calling this function
// only be called when in a state other than Follower
func (rf *Raft) stepDownToFollower(newTerm int) {
	
	rf.currentTerm = newTerm
	rf.curState = Follower
	rf.votedFor = -1
	rf.stepDownCh <- true
}

func checkError(errMsg string, err error) {
	if err != nil {
		log.Fatalf("%v: %v\n", errMsg, err)
	}
} 

func getElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(ELECTION_TIMEOUT_UPPER - ELECTION_TIMEOUT_LOWER + 1) + ELECTION_TIMEOUT_LOWER);
}

// call when lock acquired
func (rf *Raft) resetAllChannels() {
	rf.winElecCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}