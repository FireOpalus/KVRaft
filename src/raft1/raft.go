package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// raft server state for ticker
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// struct of log entries
type LogEntry struct {
	Term    int
	Command interface{}
}

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
	log []LogEntry
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
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) getLogTerm(index int) int {
	if index < 0 || index >= len(rf.log) {
		return -1
	}
	return rf.log[index].Term
}

func (rf *Raft) findConflictIndex(conflictIndex int, conflictTerm int) int {
	for idx := conflictIndex - 1; idx >= 1; idx-- {
		if rf.log[idx].Term != conflictTerm {
			break
		}
		conflictIndex = idx
	}
	return conflictIndex
}

// Find the last index with the term.
func (rf *Raft) findTerm(term int) (bool, int) {
	for i := len(rf.log) - 1; i >= 0; i-- {
		t := rf.log[i].Term
		if t == term {
			return true, i
		}
		if t < term {
			return false, -1
		}
	}
	return false, -1
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

	log.Printf("[Node %v] Becomes follower at term %v\n", rf.me, term)
}

func (rf *Raft) BecomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votersNum = 1

	log.Printf("[Node %v] Becomes candidate at term %v\n", rf.me, rf.currentTerm)
}

func (rf *Raft) BecomeLeader() {
	rf.state = Leader
	rf.votedFor = rf.me

	for idx := range rf.peers {
		rf.nextIndex[idx] = rf.getLastLogIndex() + 1
		// log.Fatalf("[Node %v] Becomes leader, next index is %v\n", rf.me, rf.nextIndex[idx])
		rf.matchIndex[idx] = 0
	}

	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	log.Printf("[Node %v] Becomes leader at term %v\n", rf.me, rf.currentTerm)
	go rf.HeartbeatBroadcast()
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
			log.Printf("[Node %v] Votes for %v at term %v\n", rf.me, args.CandidateId, args.Term)
		} else {
			reply.VoteGranted = false
			log.Printf("[Node %v] refuse to vote for %v at term %v\n", rf.me, args.CandidateId, args.Term)
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
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	ConflictTerm	int
	ConflictIndex	int
	ConflictLen 	int
}

// followers rewrite log entries by leader's appendentries RPC
// because of the linear search, if enter this function in lab 3B
// all log entries after prevlogindex need rewrite
func (rf *Raft) rewriteLogEntries(args *AppendEntriesArgs) {
	log.Printf("%v\n", rf.log)
	rf.log = rf.log[:args.PrevLogIndex + 1]
	log.Printf("%v\n", rf.log)
	rf.log = append(rf.log, args.Entries...)
	log.Printf("%v\n", rf.log)
}

// Receiver implementation
// 1.Reply false if term < currentTerm
// 2.Reply false if log doesn't contained entry at prevLogIndex whose term
// matches prevLogTerm
// 3.If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it
// 4.Append any new entries not already in the log
// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//1.Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.received = true

	if args.Term >= rf.currentTerm && rf.state != Follower {
		rf.BecomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm


	// 2.Reply false if log doesn't contained entry at prevLogIndex whose term
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictLen = len(rf.log)
		// log.Fatalf("2 %v %v %v", args.PrevLogIndex, len(rf.log), args.PrevLogTerm)
		return
	}

	// 2.Reply false if log doesn't contained entry at prevLogIndex whose term
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		reply.ConflictIndex = rf.findConflictIndex(args.PrevLogIndex, reply.ConflictTerm)
		// log.Fatalf("2 %v %v %v", args.PrevLogIndex, len(rf.log), args.PrevLogTerm)
		return
	}

	// 3. 4. not a heatbeat msg
	if args.Entries != nil {
		rf.log = rf.log[:args.PrevLogIndex + 1]
		rf.log = append(rf.log, args.Entries...)
	} 

	// 5.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}
	
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// If last log index >= nextIndex for a follower: send AppendEntries 
// RPC with log entries starting at nextIndex:
// 		If successful: update nextIndex and matchIndex for follower
//		If AppendEntries fails because of log inconsistency: decrement
//		nextIndex and retry
func (rf *Raft) MsgBroadCast(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, &args, &reply)
	for !ok && rf.state != Leader {
		if rf.killed() {
			return
		}
		ok = rf.sendAppendEntries(server, &args, &reply)
	}

	if reply.Success == true && args.Entries != nil {
		rf.matchIndex[server] += len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.CheckCommit()
	} else if reply.Success == false {
		if reply.Term > rf.currentTerm {
			rf.BecomeFollower(reply.Term)
			return
		}
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictLen
		} else {
			has, idx := rf.findTerm(reply.ConflictTerm)
			if has {
				rf.nextIndex[server] = idx + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
	}
}

// Upon election: send initial empty AppendEntries RPCs (HeartbeatBroadcast) to
// each server; repeat during idle periods to prevent election timeouts
// If command received from client: append entry to local log, repond
// after entry applied to state machine
func (rf *Raft) HeartbeatBroadcast() {
	for idx := range rf.peers {
		if rf.state != Leader {
			return 
		}
		if idx == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[idx]
		//log.Printf("Node %v's next index is %v", idx, nextIndex)
		var logEntries []LogEntry
		if nextIndex < len(rf.log) {
			// log.Fatalf("the leader's nextindex: %v, log length: %v\n", nextIndex, len(rf.log))
			logEntries = make([]LogEntry, len(rf.log[nextIndex:]))
			copy(logEntries, rf.log[nextIndex:])
		} else {
			logEntries = nil
		}
		args := AppendEntriesArgs {
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm: rf.getLogTerm(nextIndex - 1),
			Entries: logEntries,
			LeaderCommit: rf.commitIndex,
		}

		go rf.MsgBroadCast(idx, args)
	}
}

// If there exists an N such that N > commitIndex, a majority of 
// matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex
// = N
// count all servers matchIndex, if a max index that over half servers 
// have matched it, update commitIndex = N
// use Max N Scan Algorithm (Raft Official Recommands)
func (rf *Raft) CheckCommit() {
	for idx := rf.getLastLogIndex(); idx > rf.commitIndex; idx--{
		if rf.log[idx].Term != rf.currentTerm {
			continue
		}
		count := 1
		for server := range rf.peers {
			if rf.matchIndex[server] >= idx {
				count++
			}
			if count * 2 > len(rf.peers) {
				log.Printf("max n: %v\n", idx)
				rf.commitIndex = idx
				return
			}
		}
	}
	log.Printf("commitIndex now: %v\n", rf.commitIndex)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}

	newEntry := LogEntry{
		Term: rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, newEntry)
	index = rf.getLastLogIndex()
	term = rf.getLastLogTerm()

	// for (3C)
	rf.persist()

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
			rf.HeartbeatBroadcast()
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
			ms := 300 + rand.Intn(200)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

func (rf *Raft) broadcastTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.HeartbeatBroadcast()
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader && rf.received == false {
			rf.BecomeCandidate()
			rf.mu.Unlock()
			rf.MakeElection()
			rf.mu.Lock()
		}

		rf.received = false
		rf.mu.Unlock()

		ms := 150 + rand.Intn(200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		msgs := make([]raftapi.ApplyMsg, 0)

		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastLogIndex() {
			rf.lastApplied += 1
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command: rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			log.Printf("Node %v apply msg", rf.me)
			rf.applyCh <- msg
		}
		
		time.Sleep(25 * time.Millisecond)
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
	rf.log = make([]LogEntry, 1)
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
	go rf.broadcastTicker()

	go rf.electionTicker()

	go rf.applyTicker()

	return rf
}
