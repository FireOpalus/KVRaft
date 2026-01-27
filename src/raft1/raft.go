package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"

	"fmt"
)

// raft server state for ticker
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state	
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh	  chan raftapi.ApplyMsg

	// Your data here (3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor int
	state ServerState
	log []int
	votersNum int
	received bool

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int
}

//
// get node latest log term and index
//
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log) - 1]
}


//
// funcs for node to change state and initialize elements
//
func (rf *Raft) BecomeFollower(term int) {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.votersNum = 0
	}
	rf.received = true

	fmt.Printf("[Node %v] Becomes follower at term %v\n", rf.me, term)
}

func (rf *Raft) BecomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votersNum = 1

	fmt.Printf("[Node %v] Becomes candidate at term %v\n", rf.me, rf.currentTerm)
}

func (rf *Raft) BecomeLeader() {
	rf.state = Leader
	rf.votedFor = rf.me

	for idx := range rf.peers {
		rf.nextIndex[idx] = rf.getLastLogIndex() + 1
		rf.matchIndex[idx] = 0
	}

	fmt.Printf("[Node %v] Becomes leader at term %v\n", rf.me, rf.currentTerm)
	go rf.HeartBeat()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// use lock to prevent race condition
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// all structure according to the Raft paper
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

// RequestVote RPC handler.
// According to Raft paper, when requestTerm < term, respond false
// Only when voteId = null and request server is not older than requestted one
// (What is not older? It means request server's LastLogTerm and LastLogIndex is not older)
// Response votegranted = true
// Attention: update term, use newer term update older term
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.BecomeFollower(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		LastLogIndex := rf.getLastLogIndex()
		LastLogTerm := rf.getLastLogTerm()
		if LastLogTerm < args.LastLogTerm || (LastLogTerm == args.LastLogTerm && LastLogIndex <= args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.received = true
			reply.VoteGranted = true
			fmt.Printf("[Node %v] Votes for %v at term %v\n", rf.me, args.CandidateId, args.Term)
		} else {
			reply.VoteGranted = false
			fmt.Printf("[Node %v] refuse to vote for %v at term %v\n", rf.me, args.CandidateId, args.Term)
		}
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Candidates make election
// Should term increase 1 and initialize lastlogindex & lastlogterm for compare in requestvote func
// Also initilize votedFor and votersNum to get tickets and count to judge if can be leader node
func (rf *Raft) MakeElection() {
	rf.mu.Lock()
	rf.currentTerm++
	term := rf.currentTerm
	candidateid := rf.me
	lastlogindex := rf.getLastLogIndex()
	lastlogterm := rf.getLastLogTerm()
	rf.votedFor = rf.me
	rf.votersNum = 1
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{
			Term: term,
			CandidateId: candidateid,
			LastLogIndex: lastlogindex,
			LastLogTerm: lastlogterm,
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.VoteGranted == true {
				rf.votersNum++
			} else if reply.Term > rf.currentTerm {
				rf.BecomeFollower(reply.Term)
				return
			}
			if rf.state == Candidate && rf.votersNum * 2 > len(rf.peers) {
				rf.BecomeLeader()
			}
		}(idx)
	}
}

// AppendEntries structure
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.received = true

	if args.Term >= rf.currentTerm && rf.state != Follower {
		rf.BecomeFollower(args.Term)
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}
	
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) HeartBeat() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.getLastLogIndex(),
			PrevLogTerm:  rf.getLastLogTerm(),
			Entries:      []int{},
			LeaderCommit: rf.commitIndex,
		}
		// 并行发送心跳，避免阻塞
		go func(server int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply)
		}(idx, args)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state == Leader {
			rf.HeartBeat()
		} else if !rf.received {
			rf.BecomeCandidate()
			rf.mu.Unlock()
			rf.MakeElection()
			rf.mu.Lock()
		}

		rf.received = false
		rf.mu.Unlock()

		if rf.state == Leader {
			time.Sleep(100 * time.Millisecond)
		} else {
			ms := 150 + rand.Intn(200)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []int{}
	rf.state = Follower
	rf.votersNum = 0
	rf.received = true
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))	

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
