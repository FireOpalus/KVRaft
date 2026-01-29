package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

// States need to be persistent.
type PersistentState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
}

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
	received bool

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int
}

//
// get server's latest log term and index
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

//
// get server's log term which index refers to
//
func (rf *Raft) getLogTerm(index int) int {
	if index < 0 || index >= len(rf.log) {
		return -1
	}
	return rf.log[index].Term
}

//
// get the conflict index by the AppendEntriesReply.ConflictIndex
// find the first log entry which term = ConflictTerm in leader's log
// to update leader's nextIndex
//
func (rf *Raft) findConflictIndex(conflictIndex int, conflictTerm int) int {
	for idx := conflictIndex - 1; idx >= 1; idx-- {
		if rf.log[idx].Term != conflictTerm {
			break
		}
		conflictIndex = idx
	}
	return conflictIndex
}

//
// Find the last index with the term parameter .
// used to find the index of the last log entry of the specified term in Raft's log.
// 
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
// funcs for server to change state and initialize elements
//
func (rf *Raft) BecomeFollower(term int) {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.persist()
	rf.received = true

	// log.Printf("[Node %v] Becomes follower at term %v\n", rf.me, term)
}

func (rf *Raft) BecomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	// log.Printf("[Node %v] Becomes candidate at term %v\n", rf.me, rf.currentTerm)
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
	// log.Printf("[Node %v] Becomes leader at term %v\n", rf.me, rf.currentTerm)

	// when state change complete, broadcast heartbeat immediately
	rf.HeartbeatBroadcast()
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
	rf.persistSnapshot(rf.persister.ReadSnapshot())
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(PersistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
	})
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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
	if len(data) == 0 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ps PersistentState
	if d.Decode(&ps) != nil {
		log.Printf("Fail to decode.\n")
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = ps.CurrentTerm
		rf.votedFor = ps.VotedFor
		rf.log = ps.Log
	}
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

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.BecomeFollower(args.Term)
	} else if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.BecomeFollower(args.Term)
	}
	upToDate := (args.LastLogTerm > rf.getLastLogTerm()) ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())

	// log.Printf("[Node %v] received vote req from %v at term %v ", rf.me, args.CandidateID, args.CurrentTerm)
	// log.Printf("rf.votedFor = %v, upToDate = %v\n", rf.votedFor, upToDate)

	if upToDate {
		rf.received = true
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			// Vote for this candidate, and reset rf.received.
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.persist()
			return
		}
	}
	// Refuse to vote.
	reply.VoteGranted = false
	
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, count *int32) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()

		if reply.VoteGranted {
			atomic.AddInt32(count, 1)
			// log.Printf("[Node %v] received vote from %v at term %v, count = %v\n", rf.me, server, args.CurrentTerm, atomic.LoadInt32(count))
			if rf.state == Candidate && atomic.LoadInt32(count) > int32(len(rf.peers)/2) {
				rf.mu.Unlock()
				rf.BecomeLeader()
				rf.mu.Lock()
			}
		} else if reply.Term > rf.currentTerm {
			rf.BecomeFollower(reply.Term)
		}

		rf.mu.Unlock()
	} else {
		// log.Printf("[Node %v] failed to send request vote to %v\n", rf.me, server)
	}
}

// Candidates make election
// Should term increase 1 and initialize lastlogindex & lastlogterm for compare in requestvote func
// Also initilize votedFor and votersNum to get tickets and count to judge if can be leader node
func (rf *Raft) MakeElection() {
	arg := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:  rf.currentTerm,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	var count int32 = 1

	for i := 0; i < len(rf.peers); i++ {
		if rf.state != Candidate {
			break
		}
		if i == rf.me {
			continue
		}
		var reply RequestVoteReply
		go rf.sendRequestVote(i, &arg, &reply, &count)
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

	// log.Printf("[Node %v] received append entries from %v at term %v\n", rf.me, args.LeaderID, args.Term)

	// leader's term is too old.
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

	// Check if PrevLogIndex exists.
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictLen = len(rf.log)
		return
	}
	// Check logEntry's term.
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		reply.ConflictIndex = rf.findConflictIndex(args.PrevLogIndex, reply.ConflictTerm)
		return
	}

	// followers rewrite log entries by leader's appendentries RPC
	// because of the linear search, if enter this function in lab 3B
	// all log entries after prevlogindex need rewrite
	rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) HeartbeatBroadcast() {
	for i := range rf.peers {
		if rf.state != Leader {
			// "If a leader or candidate discovers a server with a higher term, it immediately reverts to follower state."
			return
		}
		if i == rf.me {
			continue
		}

		go func(i int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				nextIndex := rf.nextIndex[i]

				var logEntries []LogEntry
				if nextIndex < len(rf.log) {
					logEntries = make([]LogEntry, len(rf.log[nextIndex:]))
					copy(logEntries, rf.log[nextIndex:])
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: max(rf.nextIndex[i]-1, 0),
					PrevLogTerm:  rf.getLogTerm(rf.nextIndex[i] - 1),
					Entries:      logEntries,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(i, &args, &reply)

				for !ok && rf.state == Leader {
					if rf.killed() {
						return
					}
					ok = rf.sendAppendEntries(i, &args, &reply) // Retry sending.
				}
				rf.mu.Lock()
				if rf.state != Leader || rf.killed() {
					rf.mu.Unlock()
					return
				}

				// Check if follower has higher term.
				if reply.Term > rf.currentTerm {
					rf.BecomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				// If follower's log is up-to-date, update nextIndex and matchIndex.
				if reply.Success {
					// log.Printf("[Node %v] Append entries to Node [%v] at term %v\n", rf.me, i, rf.currentTerm)
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.CheckCommit()
					rf.mu.Unlock()
					return
				} else if rf.state != Leader {
					// If this node is not leader anymore, return.
					rf.mu.Unlock()
					return
				} else {
					// Unsuccessful, decrease nextIndex and retry.
					if reply.ConflictTerm != -1 {
						has, idx := rf.findTerm(reply.ConflictTerm)
						if has {
							rf.nextIndex[i] = idx + 1
						} else {
							rf.nextIndex[i] = reply.ConflictIndex
						}
					} else {
						rf.nextIndex[i] = reply.ConflictLen
					}
					rf.mu.Unlock()
					// keep looping until success.
				}
			}
		}(i)
	}
}

// If there exists an N such that N > commitIndex, a majority of 
// matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex
// = N


// count all servers matchIndex, if a max index that over half servers 
// have matched it, update commitIndex = N
// use Max N Scan Algorithm (Raft Official Recommands)
/* for idx := rf.getLastLogIndex(); idx > rf.commitIndex; idx--{
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
	} */

// get all matchIndex and sort them, the middle num is the popular num
func (rf *Raft) CheckCommit() {
	rf.matchIndex[rf.me] = rf.getLastLogIndex()

	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	sort.Ints(matchIndexCopy)
	
	index := matchIndexCopy[len(matchIndexCopy)/2]

	if index > rf.commitIndex && rf.getLogTerm(index) == rf.currentTerm {
		rf.commitIndex = index
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

// use three ticker to broadcast, election and apply

func (rf *Raft) broadcastTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.HeartbeatBroadcast()
		}
		rf.mu.Unlock()
		time.Sleep(25 * time.Millisecond)
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader && rf.received == false {
			rf.BecomeCandidate()
			rf.MakeElection()
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
			// log.Printf("Node %v apply msg", rf.me)
			rf.applyCh <- msg
		}
		
		time.Sleep(10 * time.Millisecond)
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
