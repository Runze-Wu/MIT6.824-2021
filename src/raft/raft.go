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
	currentTerm int   // latest term server has seen
	voteFor     int   // candidateId that received vote in current term
	log         []Log // log entries, index 0 store the snapshot info
	/* Volatile state on all servers */
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine
	/* Volatile state on leaders */
	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of the highest log entry known to replicated on server

	/* Other state used for implementation */
	role           Role          // the server's role
	stopCh         chan bool     // dead signal
	applyCh        chan ApplyMsg // channel which send apply msg
	notifyApplyCh  chan struct{} // channel which notify to send apply
	applyTimer     *time.Timer   // apply msg timer
	electionTimer  *time.Timer   // election time-out timer
	heartBeatTimer *time.Timer   // appendEntries timer

	/* debug info record lock interval */
	lockStart time.Time
	lockEnd   time.Time
	lockName  string
}

func (rf *Raft) lock(lockName string) {
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockName = lockName
}

func (rf *Raft) unLock(lockName string) {
	rf.lockEnd = time.Now()
	assert(rf.lockName == lockName, "lock not matched for %s : %s", rf.lockName, lockName)
	rf.lockName = ""
	duration := rf.lockEnd.Sub(rf.lockStart)
	if rf.lockName != "" && duration > MaxLockTime {
		rf.printState()
		ERROR("lock too long:%s:%s", lockName, duration)
	}
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock("GetState")
	defer rf.unLock("GetState")
	// Your code here (2A).
	return rf.currentTerm, rf.role == Leader
}

//
// get the snapshot which stores lastIncluded index and term
//
func (rf *Raft) getSnapshot() Log {
	return rf.log[0]
}

//
// get the last log entry's index
//
func (rf *Raft) getLastLogIndex() int {
	index := rf.log[len(rf.log)-1].Index
	assert(len(rf.log)-1+rf.getSnapshot().Index == index,
		"last index mismatched logLen %d snapIndex %d storedIndex %d", len(rf.log), rf.getSnapshot().Index, index)
	return index
}

//
// get the first log entry's index
//
func (rf *Raft) getLogStartIndex() int {
	return rf.getSnapshot().Index
}

//
// get the indexed log
//
func (rf *Raft) getLogByIndex(index int) Log {
	assert(index >= rf.getLogStartIndex(), "index %d, but startIdx %d", index, rf.getLogStartIndex())
	return rf.log[index-rf.getLogStartIndex()]
}

//
// get the idx point to the index's entry
//
func (rf *Raft) getIdx(index int) int {
	return index - rf.getLogStartIndex()
}

//
// reset the electionTimer
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
//
func (rf *Raft) setHeartBeatTimer() {
	rf.heartBeatTimer.Stop()
	rf.heartBeatTimer.Reset(HeartBeatTimeout)
}

//
// non-leader needn't send heartBeat msg
//
func (rf *Raft) stopHeartBeatTimer() {
	rf.heartBeatTimer.Stop()
}

//
// meet new term change server's role to follower
//
func (rf *Raft) stepDown(newTerm int) {
	assert(newTerm >= rf.currentTerm, "server currentTerm %d is bigger than newTerm %d", rf.currentTerm, newTerm)
	isLeader := rf.role == Leader
	if newTerm > rf.currentTerm {
		VERBOSE("server %d, stepDown(%d)", rf.me, newTerm)
		rf.currentTerm = newTerm
		rf.voteFor = -1
		rf.role = Follower
		rf.printState()
	} else {
		if rf.role != Follower {
			rf.role = Follower
			rf.printState()
		}
	}
	if isLeader {
		rf.setElectionTimer(randomElectionTime()) // restart election timer
	}
	rf.stopHeartBeatTimer() // stop heartBeat timers
}

//
// print server's useful state info
//
func (rf *Raft) printState() {
	NOTICE("server=%v, term=%v, role=%v, vote=%v, "+
		"commit=%v, applied=%v, snap=%v",
		rf.me, rf.currentTerm, rf.role, rf.voteFor, rf.commitIndex, rf.lastApplied, rf.getSnapshot())
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
	// Your code here (2B).
	rf.lock("Start")
	defer rf.unLock("Start")
	index := rf.getLastLogIndex()
	term := rf.currentTerm
	index++
	isLeader := rf.role == Leader
	if isLeader {
		rf.log = append(rf.log, Log{
			EntryType: Data, Command: command,
			Term: term, Index: index,
		})
		rf.matchIndex[rf.me] = index // keep its own match index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
	}
	go rf.replicate()
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

func (rf *Raft) applyThreadMain() {
	for rf.killed() == false {
		select {
		case <-rf.stopCh:
			return
		case <-rf.applyTimer.C:
			rf.notifyApplyCh <- struct{}{}
		case <-rf.notifyApplyCh:
			rf.startApply()
		}
	}
}

func (rf *Raft) startApply() {
	rf.lock("StartApply")
	var sendMsgs []ApplyMsg
	rf.applyTimer.Reset(ApplyInterval)
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			idx := rf.getIdx(i)
			assert(rf.log[idx].Index == i, "send apply index not matched")
			sendMsgs = append(sendMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[idx].Command,
				CommandTerm:  rf.log[idx].Term,
				CommandIndex: rf.log[idx].Index,
			})
		}
	} else {
		sendMsgs = make([]ApplyMsg, 0)
	}
	rf.unLock("StartApply")
	for _, msg := range sendMsgs {
		rf.applyCh <- msg
		rf.lock("SendMsg")
		rf.printState()
		VERBOSE("send %v idx:%d", msg.Command, msg.CommandIndex)
		rf.lastApplied = msg.CommandIndex
		rf.unLock("SendMsg")
	}
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
	rand.Seed(time.Now().UTC().UnixNano()) // initialize rand seed for setting timer
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		role:           Follower,
		currentTerm:    0,
		voteFor:        -1,
		log:            make([]Log, 1), // first index is 1
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		stopCh:         make(chan bool),
		applyCh:        applyCh,
		notifyApplyCh:  make(chan struct{}, 100),
		applyTimer:     time.NewTimer(ApplyInterval),
		electionTimer:  time.NewTimer(randomElectionTime()),
		heartBeatTimer: time.NewTimer(HeartBeatTimeout),
	}
	rf.readPersist(persister.ReadRaftState())
	// Your initialization code here (2A, 2B, 2C).
	rf.stopHeartBeatTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start send apply msg goroutine
	go rf.applyThreadMain()
	return rf
}
