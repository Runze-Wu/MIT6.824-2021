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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type EntryType int

const (
	Unknown EntryType = 0
	Data    EntryType = 1
	Noop    EntryType = 2
)

type Log struct {
	EntryType EntryType
	Command   interface{}
	Term      int
	Index     int
}

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

const (
	ElectionTimeout  = time.Millisecond * 300 // election
	HeartBeatTimeout = time.Millisecond * 150 // send no more than ten times per sec
	ApplyInterval    = time.Millisecond * 100 // apply log
	RPCTimeout       = time.Millisecond * 100 // periodically send rpc when send fails
	MaxLockTime      = time.Millisecond * 10  // debug
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* Persistent state */
	role        Role  // the server's role
	currentTerm int   // latest term server has seen
	voteFor     int   // candidateId that received vote in current term
	log         []Log // log entries
	/* Volatile state on all servers */
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine
	/* Volatile state on leaders */
	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of the highest log entry known to replicated on server

	/* Other state used for implementation */
	stopCh         chan bool     // dead signal
	leaderId       int           // current leader
	applyCh        chan ApplyMsg // channel which send apply msg
	electionTimer  *time.Timer   // election time-out timer
	heartBeatTimer *time.Timer   // appendEntries timer

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.role == Leader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// get the last log entry's index and term
// thread already own the lock
//
func (rf *Raft) getLastLogIndexTerm() (int, int) {
	return rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLogStartIndex() int {
	ERROR("haven't implemented")
	return -1
}

func (rf *Raft) getLogByIndex(index int) Log {
	return rf.log[index]
}

func randomElectionTime() time.Duration {
	return time.Duration(rand.Int())%ElectionTimeout + ElectionTimeout
}

//
// reset the electionTimer
// thread already own the lock
//
func (rf *Raft) setElectionTimer(d time.Duration) {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(d)
}

//
// current leader needn't the electionTimer
//
func (rf *Raft) stopElectionTimer() {
	rf.electionTimer.Stop()
}

//
// reset the heartBeatTimers
// thread already own the lock
//
func (rf *Raft) setHeartBeatTimer() {
	rf.heartBeatTimer.Stop()
	rf.heartBeatTimer.Reset(HeartBeatTimeout)

	VERBOSE("server %d in term %d role %d start HB", rf.me, rf.currentTerm, rf.role)
}

//
// non-leader needn't send heartBeat msg
//
func (rf *Raft) stopHeartBeatTimer() {
	rf.heartBeatTimer.Stop()

	VERBOSE("server %d in term %d role %d stop HB", rf.me, rf.currentTerm, rf.role)
}

//
// meet new term change server's role to follower
// thread already own the lock
//
func (rf *Raft) stepDown(newTerm int) {
	if newTerm < rf.currentTerm {
		ERROR("server currentTerm %d is bigger than newTerm %d",
			rf.currentTerm, newTerm)
	}
	isLeader := rf.role == Leader
	if newTerm > rf.currentTerm {
		VERBOSE("server %d, stepDown(%d)", rf.me, newTerm)
		rf.currentTerm = newTerm
		rf.voteFor = -1
		rf.role = Follower
		rf.printElectionState()
	} else {
		if rf.role != Follower {
			rf.role = Follower
			rf.printElectionState()
		}
	}
	if isLeader {
		rf.setElectionTimer(randomElectionTime()) // restart election timer
	}
	rf.stopHeartBeatTimer() // stop heartBeat timers
}

//
// print server's useful state info
// thread already own the lock
//
func (rf *Raft) printElectionState() {
	s := ""
	switch rf.role {
	case Follower:
		s = "FOLLOWER"
	case Candidate:
		s = "CANDIDATE"
	case Leader:
		s = "LEADER"
	}
	NOTICE("server=%d, term=%d, role=%s, vote=%d",
		rf.me, rf.currentTerm, s, rf.voteFor)
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
	close(rf.stopCh)
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
	rand.Seed(time.Now().UTC().UnixNano()) // initialize rand seed for setting timer
	rf.dead = 0
	rf.role = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]Log, 1) // first index is 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.stopCh = make(chan bool)
	rf.leaderId = -1
	rf.applyCh = make(chan ApplyMsg)
	rf.electionTimer = time.NewTimer(randomElectionTime())
	rf.heartBeatTimer = time.NewTimer(HeartBeatTimeout)
	rf.stopHeartBeatTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
