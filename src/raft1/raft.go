package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogType struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedID     int
	log         []LogType

	commitIndex int
	lastApplied int

	nextIndex       []int
	matchIndex      []int
	lastReplication []time.Time
	votes           int

	state string //"Follower","Candidate","Leader"

	electionTimeout   time.Duration
	lastElectionReset time.Time
	lastHeartbeat     time.Time

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond

	lastindex int
	snapshot  []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = (rf.state == "Leader")
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedID)
	e.Encode(rf.log)
	e.Encode(rf.lastindex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedID int
	var log []LogType
	var lastindex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedID) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastindex) != nil {
		fmt.Printf("cannot read from presister\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedID = votedID
		rf.log = log
		rf.lastindex = lastindex
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.commitIndex = lastindex
		rf.lastApplied = lastindex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.log) == 0 || index <= rf.log[0].Index ||
		index > rf.log[len(rf.log)-1].Index {
		return
	}

	offset := index - rf.log[0].Index
	rf.log = append([]LogType(nil), rf.log[offset:]...)
	rf.lastindex = index
	rf.snapshot = append([]byte(nil), snapshot...)

	rf.persist()
}

type InstallSnapshotArgs struct {
	Term         int
	LeaderID     int
	Index        int
	SnapshotTerm int
	Snapshot     []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedID = -1
	}
	rf.state = "Follower"
	rf.resetElectionTime()
	reply.Term = rf.currentTerm

	base := rf.log[0].Index
	if args.Index <= base {
		rf.persist()
		reply.Success = true
		return
	}

	offset := args.Index - base
	if offset < len(rf.log) &&
		rf.log[offset].Index == args.Index &&
		rf.log[offset].Term == args.SnapshotTerm {
		rf.log = append([]LogType(nil), rf.log[offset:]...)
	} else if args.Index > rf.commitIndex {
		rf.log = []LogType{{Index: args.Index, Term: args.SnapshotTerm}}
	} else {
		// Do not discard committed entries when an older snapshot arrives.
		rf.persist()
		reply.Success = true
		return
	}

	rf.lastindex = args.Index
	rf.snapshot = append([]byte(nil), args.Snapshot...)
	if rf.commitIndex < args.Index {
		rf.commitIndex = args.Index
	}
	rf.persist()
	rf.applyCond.Signal()
	reply.Success = true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	Lastlogindex int
	Lastlogterm  int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedID = -1
		rf.state = "Follower"
		rf.persist()
	}
	reply.Term = rf.currentTerm
	if rf.state == "Follower" && (rf.votedID == -1 || rf.votedID == args.CandidateID) {
		if len(rf.log) > 0 && (args.Lastlogterm < rf.log[len(rf.log)-1].Term ||
			(args.Lastlogterm == rf.log[len(rf.log)-1].Term && args.Lastlogindex < rf.log[len(rf.log)-1].Index)) {
			reply.VoteGranted = false
			return
		} else {
			rf.votedID = args.CandidateID
			rf.resetElectionTime()
			rf.persist()
			reply.VoteGranted = true
			return
		}
	} else {
		reply.VoteGranted = false
		return
	}
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

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogType
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) resetElectionTime() {
	rf.lastElectionReset = time.Now()
	rf.electionTimeout = time.Duration(400+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedID = -1
		rf.state = "Follower"
		rf.persist()
	}

	reply.Term = rf.currentTerm
	rf.persist()
	reply.Success = true
	rf.state = "Follower"
	rf.resetElectionTime()

	startindex := rf.log[0].Index
	lastindex := rf.log[len(rf.log)-1].Index
	if args.PrevLogIndex < startindex {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = startindex + 1
		return
	}
	if args.PrevLogIndex > lastindex {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = lastindex + 1
		return
	}

	if rf.log[args.PrevLogIndex-startindex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex-startindex].Term
		reply.XLen = lastindex + 1

		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > startindex &&
			rf.log[reply.XIndex-1-startindex].Term == reply.XTerm {
			reply.XIndex--
		}

		return
	}

	if len(args.Entries) > 0 {
		insertindex := args.PrevLogIndex + 1
		entryindex := 0

		for insertindex <= lastindex && entryindex < len(args.Entries) {
			if rf.log[insertindex-startindex].Term != args.Entries[entryindex].Term {
				rf.log = rf.log[:insertindex-startindex]
				break
			}
			insertindex++
			entryindex++
		}

		rf.log = append(rf.log, args.Entries[entryindex:]...)

		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	index := rf.log[len(rf.log)-1].Index + 1
	term := rf.currentTerm
	isLeader := (rf.state == "Leader")

	if rf.state != "Leader" {
		rf.mu.Unlock()
		return -1, term, false
	}

	// Your code here (3B).
	entry := LogType{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index

	rf.persist()

	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me {
			rf.triggerReplication(server)
		}
	}

	return index, term, isLeader
}

func (rf *Raft) triggerReplication(peer int) {
	rf.mu.Lock()
	if rf.state != "Leader" {
		rf.mu.Unlock()
		return
	}
	if time.Since(rf.lastReplication[peer]) < 100*time.Millisecond {
		rf.mu.Unlock()
		return
	}
	rf.lastReplication[peer] = time.Now()
	rf.mu.Unlock()

	go rf.ReplicateToPeer(peer)
}

func (rf *Raft) ReplicateToPeer(peer int) {
	for {
		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm
		startindex := rf.log[0].Index
		lastLogIndex := rf.log[len(rf.log)-1].Index
		nextIndex := rf.nextIndex[peer]
		if nextIndex < 1 {
			nextIndex = 1
		}

		if startindex > 0 && nextIndex <= startindex {
			args := &InstallSnapshotArgs{
				Term:         term,
				LeaderID:     rf.me,
				Index:        startindex,
				SnapshotTerm: rf.log[0].Term,
				Snapshot:     append([]byte(nil), rf.snapshot...),
			}
			rf.mu.Unlock()

			reply := &InstallSnapshotReply{}
			if !rf.peers[peer].Call("Raft.InstallSnapshot", args, reply) {
				return
			}

			rf.mu.Lock()
			if rf.state != "Leader" || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = "Follower"
				rf.votedID = -1
				rf.resetElectionTime()
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if reply.Success && args.Index > rf.matchIndex[peer] {
				rf.matchIndex[peer] = args.Index
				rf.nextIndex[peer] = args.Index + 1
			}
			moreEntries := reply.Success &&
				rf.nextIndex[peer] <= rf.log[len(rf.log)-1].Index
			rf.mu.Unlock()
			if !moreEntries {
				return
			}
			continue
		}

		if nextIndex > lastLogIndex+1 {
			nextIndex = lastLogIndex + 1
			rf.nextIndex[peer] = nextIndex
		}
		prevLogIndex := nextIndex - 1
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex-startindex].Term,
			Entries:      append([]LogType(nil), rf.log[nextIndex-startindex:]...),
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()
		reply := &AppendEntriesReply{}

		ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		if rf.state != "Leader" || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = "Follower"
			rf.votedID = -1
			rf.resetElectionTime()
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if !reply.Success && args.PrevLogIndex <= rf.matchIndex[peer] {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)

			if match > rf.matchIndex[peer] {
				rf.matchIndex[peer] = match
				rf.nextIndex[peer] = match + 1
			}

			moreEntries := rf.nextIndex[peer] <= rf.log[len(rf.log)-1].Index
			rf.mu.Unlock()
			if !moreEntries {
				return
			}
			continue
		}

		if !reply.Success {
			if reply.XTerm == -1 {
				rf.nextIndex[peer] = reply.XLen
			} else {
				lastIndex := -1

				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.XTerm {
						lastIndex = rf.log[i].Index
						break
					}
				}

				if lastIndex >= 0 {
					rf.nextIndex[peer] = lastIndex + 1
				} else {
					rf.nextIndex[peer] = reply.XIndex
				}
			}
			if rf.nextIndex[peer] < 1 {
				rf.nextIndex[peer] = 1
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) StartElection() {
	rf.state = "Candidate"
	rf.votedID = rf.me
	rf.votes = 1
	rf.currentTerm++
	electionTerm := rf.currentTerm
	rf.resetElectionTime()
	rf.persist()

	var args RequestVoteArgs
	if len(rf.log) > 0 {
		last := rf.log[len(rf.log)-1]
		args = RequestVoteArgs{electionTerm, rf.me, last.Index, last.Term}
	} else {
		args = RequestVoteArgs{electionTerm, rf.me, 0, 0}
	}

	becameLeader := rf.votes >= len(rf.peers)/2+1
	if becameLeader {
		rf.state = "Leader"
		rf.lastHeartbeat = time.Now()
	}
	rf.mu.Unlock()

	if becameLeader {
		rf.sendHeartbeat(electionTerm)
	}

	for i := range rf.peers {
		if i != rf.me {
			go func(server int, args RequestVoteArgs) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, reply)
				if !ok {
					return
				}

				becameLeader := false
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.state = "Follower"
					rf.votedID = -1
					rf.currentTerm = reply.Term
					rf.resetElectionTime()
					rf.persist()
				} else if reply.Term == electionTerm && reply.VoteGranted &&
					rf.state == "Candidate" && rf.currentTerm == electionTerm {
					rf.votes++
					if rf.votes >= len(rf.peers)/2+1 {
						for peer := range rf.peers {
							rf.nextIndex[peer] = rf.log[len(rf.log)-1].Index + 1
							rf.matchIndex[peer] = 0
						}
						rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index

						rf.state = "Leader"
						rf.lastHeartbeat = time.Now()
						becameLeader = true
					}
				}
				rf.mu.Unlock()

				if becameLeader {
					rf.sendHeartbeat(electionTerm)
				}
			}(i, args)
		}
	}

	rf.mu.Lock()
}

func (rf *Raft) sendHeartbeat(term int) {
	rf.mu.Lock()
	if rf.state != "Leader" || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me {
			rf.triggerReplication(server)
		}
	}
}

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for n := len(rf.log) - 1; rf.log[n].Index > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		totalPeer := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= rf.log[n].Index {
				totalPeer++
			}
		}

		if totalPeer >= len(rf.peers)/2+1 {
			rf.commitIndex = rf.log[n].Index
			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex &&
			rf.lastApplied >= rf.log[0].Index {
			rf.applyCond.Wait()
		}

		if rf.lastApplied < rf.log[0].Index {
			index := rf.log[0].Index
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      append([]byte(nil), rf.snapshot...),
				SnapshotIndex: index,
				SnapshotTerm:  rf.log[0].Term,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg

			rf.mu.Lock()
			if rf.lastApplied < index {
				rf.lastApplied = index
			}
			rf.mu.Unlock()
			continue
		}

		startindex := rf.log[0].Index
		index := rf.lastApplied + 1
		entry := rf.log[index-startindex]
		rf.mu.Unlock()

		msg := raftapi.ApplyMsg{
			CommandValid: true,
			CommandIndex: index,
			Command:      entry.Command,
		}

		rf.applyCh <- msg

		rf.mu.Lock()
		if rf.lastApplied < index {
			rf.lastApplied = index
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != "Leader" && time.Since(rf.lastElectionReset) >= rf.electionTimeout {
			rf.StartElection()
		}
		rf.mu.Unlock()

		time.Sleep(50 * time.Millisecond)

		var SendHeartbeat bool
		var term int
		rf.mu.Lock()
		if rf.state == "Leader" && time.Since(rf.lastHeartbeat) >= 100*time.Millisecond {
			rf.lastHeartbeat = time.Now()
			term = rf.currentTerm
			SendHeartbeat = true
		}
		rf.mu.Unlock()

		if SendHeartbeat {
			rf.sendHeartbeat(term)
		}

		rf.mu.Lock()
		isLeader := rf.state == "Leader"
		rf.mu.Unlock()
		if isLeader {
			rf.commitLog()
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
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastindex = 0

	rf.votedID = -1
	rf.state = "Follower"

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastReplication = make([]time.Time, len(rf.peers))
	for peer := range rf.peers {
		rf.nextIndex[peer] = 1
	}

	// Your initialization code here (3A, 3B, 3C).
	rf.lastElectionReset = time.Now()
	rf.electionTimeout = time.Duration(400+rand.Intn(300)) * time.Millisecond
	rf.applyCh = applyCh
	rf.log = []LogType{
		{Index: 0, Term: 0, Command: nil},
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
