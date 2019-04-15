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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

// LogEntry Info
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply Info
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs Info
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply Info
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	// volatile state on servers
	commitIndex int
	lastApplied int
	// volatile state on leader
	nextIndex        []int
	matchIndex       []int
	state            int
	voteCount        int
	mostRecentLeader int
	chanElection     chan bool
	chanHeartbeat    chan bool
	chanBeLeader     chan bool
	chanCommitLog    chan bool
}

// server auth
const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
	HEARTBEAT = 50 * time.Millisecond
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// type InstallSnapshotArgs struct {
// 	Term             int
// 	LeaderID         int
// 	LastIncludeIndex int
// 	LastIncludeTerm  int
// 	Data             []byte
// }

// type InstallSnapshotReply struct {
// 	Term int
// }

// func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
// 	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if ok {
// 		if reply.Term > args.Term {
// 			rf.currentTerm = reply.Term
// 			rf.state = FOLLOWER
// 			rf.votedFor = -1
// 			return ok
// 		}
// 		rf.nextIndex[server] = args.LastIncludeIndex + 1
// 		rf.matchIndex[server] = args.LastIncludeIndex
// 	}
// 	return ok
// }

// // CutOffLog
// func (rf *Raft) CutOffLog(LastIncludeIndex int, LastIncludeTerm int) {

// 	var newLogEntries []LogEntry

// 	newLogEntries = append(newLogEntries, LogEntry{Term: LastIncludeTerm, Index: LastIncludeIndex})

// 	for i := len(rf.log) - 1; i >= 0; i-- {
// 		if i == LastIncludeIndex && rf.log[i].Term == LastIncludeTerm {
// 			rf.log = append(newLogEntries, (rf.log[i+1:])...)
// 			break
// 		}
// 	}

// 	rf.log = newLogEntries
// }

// func (rf *Raft) StartSnapShot(index int) {
// 	DPrintf("!!\n")
// 	baseIndex := rf.log[0].Index
// 	lastIndex := rf.getLastIndex()
// 	DPrintf("baseIndex: %d, index : %d, lastIndex: %d\n", baseIndex, index, lastIndex)
// 	if index <= baseIndex || index > lastIndex {
// 		return
// 	}

// 	var newLogEntries []LogEntry
// 	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: rf.log[index-baseIndex].Term})

// 	for i := index + 1; i <= lastIndex; i++ {
// 		newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
// 	}

// 	rf.log = newLogEntries
// 	w := new(bytes.Buffer)
// 	e := gob.NewEncoder(w)
// 	e.Encode(newLogEntries[0].Index)
// 	e.Encode(newLogEntries[0].Term)
// 	data := w.Bytes()
// 	DPrintf("??\n")
// 	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), data)
// 	DPrintf("??\n")
// }

// func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	DPrintf("InstallSnapshot")
// 	reply.Term = rf.currentTerm

// 	if args.Term < rf.currentTerm {
// 		return
// 	}

// 	rf.chanElection <- true
// 	rf.state = FOLLOWER
// 	rf.CutOffLog(args.LastIncludeIndex, args.LastIncludeTerm)

// 	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
// 	rf.commitIndex = args.LastIncludeIndex
// 	rf.lastApplied = args.LastIncludeIndex

// 	rf.readSnapShot(args.Data)
// 	rf.persist()
// }

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// func (rf *Raft) readSnapShot(data []byte) {
// 	// DPrintf("readSnapShot\n")
// 	rf.readPersist(rf.persister.ReadRaftState())

// 	if len(data) == 0 {
// 		return
// 	}
// 	r := bytes.NewBuffer(data)
// 	d := gob.NewDecoder(r)

// 	var lastIncludeIndex int
// 	var lastIncludeTerm int
// 	d.Decode(&lastIncludeIndex)
// 	d.Decode(&lastIncludeTerm)

// 	rf.commitIndex = lastIncludeIndex
// 	rf.lastApplied = lastIncludeIndex
// 	DPrintf("lastIncludeIndex: %d, lastIncludeTerm: %d\n", lastIncludeIndex, lastIncludeTerm)

// 	rf.CutOffLog(lastIncludeIndex, lastIncludeTerm)
// 	DPrintf("readSnapShot\n")

// }

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// AppendEntries or HeartBeat Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.mostRecentLeader = args.LeaderID
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	rf.chanHeartbeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	reply.Term = args.Term

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[args.PrevLogIndex].Term != rf.log[i].Term {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}

	rf.log = (rf.log)[:(args.PrevLogIndex + 1)]
	rf.log = append(rf.log, (args.Entries)...)

	reply.Success = true
	reply.NextIndex = rf.getLastIndex() + 1

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		rf.chanCommitLog <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != LEADER || args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.persist()
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DPrintf("%d 收到 %d 的附加RPC: %v\n", server, args.LeaderID, *args)
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

// BoardCastAppendEntries From Leader
func (rf *Raft) BoardCastAppendEntries() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	i := rf.commitIndex
	last := rf.getLastIndex()

	for N := rf.commitIndex + 1; N <= last; N++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= N && rf.log[N].Term == rf.currentTerm {
				num = num + 1
			}
		}

		if 2*num > len(rf.peers) {
			rf.commitIndex = N
		}
	}

	if i != rf.commitIndex {
		rf.persist()
		rf.chanCommitLog <- true
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == LEADER {

			// if rf.nextIndex[i] > baseIndex {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderID = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			// if args.PrevLogIndex > rf.getLastIndex() {
			// 	DPrintf("log.lastIndex %d ; PrevLogIndex: %d\n", rf.getLastIndex(), args.PrevLogIndex)
			// }

			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = (rf.log)[rf.nextIndex[i]:]
			args.LeaderCommit = rf.commitIndex
			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, &args, &reply)

			}(i, args)
			// } else {
			// 	var args InstallSnapshotArgs
			// 	DPrintf("BeginSnapShot\n")

			// 	args.LastIncludeIndex = rf.log[0].Index
			// 	args.LastIncludeTerm = rf.log[0].Term
			// 	args.LeaderID = rf.me
			// 	args.Term = rf.currentTerm
			// 	args.Data = rf.persister.snapshot

			// 	go func(i int, args InstallSnapshotArgs) {
			// 		var reply InstallSnapshotReply
			// 		rf.sendInstallSnapshot(i, &args, &reply)
			// 		DPrintf("FinishedSnapShot\n")
			// 	}(i, args)
			// }
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm

	upToDate := false
	if args.LastLogTerm > rf.getLastTerm() {
		upToDate = true
	}

	if args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex() {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.state = FOLLOWER
		rf.chanElection <- true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != CANDIDATE || rf.currentTerm != args.Term {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.persist()
			rf.chanElection <- true
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount = rf.voteCount + 1
			if rf.state == CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				rf.state = LEADER
				rf.chanBeLeader <- true
			}
		}
	}
	return ok
}

// BoardCastRequestVote From Candidate
func (rf *Raft) BoardCastRequestVote() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	args.CandidateID = rf.me

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
			}(i)
		}
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		isLeader = false
	} else {
		index = rf.getLastIndex() + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		// rf.StartSnapShot(2)
		DPrintf("命令是: %v\n", command)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// initialize from state persisted before a crash

	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.chanBeLeader = make(chan bool)
	rf.chanElection = make(chan bool)
	rf.chanHeartbeat = make(chan bool)
	rf.chanCommitLog = make(chan bool)

	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	DPrintf("%d　开始启动 currentTerm: %d\n", rf.me, rf.currentTerm)
	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				select {
				case <-rf.chanHeartbeat:
				case <-rf.chanElection:
				case <-time.After(time.Duration(rand.Intn(200)+350) * time.Millisecond):
					rf.mu.Lock()
					rf.state = CANDIDATE
					rf.mu.Unlock()
				}
			case LEADER:
				// DPrintf("%d 广播心跳\n", rf.me)
				rf.BoardCastAppendEntries()
				select {
				case <-rf.chanHeartbeat:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.mu.Unlock()
				case <-rf.chanElection:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.mu.Unlock()
				case <-time.After(HEARTBEAT):
				}
			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm = rf.currentTerm + 1
				DPrintf("%d 成为候选人, currentTerm: %d\n", rf.me, rf.currentTerm)
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				rf.BoardCastRequestVote()
				select {
				case <-time.After(time.Duration(rand.Intn(200)+350) * time.Millisecond):
				case <-rf.chanHeartbeat:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.mu.Unlock()
				case <-rf.chanElection:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.mu.Unlock()
				case <-rf.chanBeLeader:
					rf.mu.Lock()
					rf.state = LEADER
					DPrintf("%d 成为Leader\n", rf.me)
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}

		}
	}()

	go func() {
		for {
			select {
			case <-rf.chanCommitLog:
				rf.mu.Lock()
				// baseIndex := rf.log[0].Index
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
				}
				rf.lastApplied = rf.commitIndex
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
