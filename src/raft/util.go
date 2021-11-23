package raft

import (
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"sort"
	"time"
)

const (
	Error   bool = true
	Warning bool = false
	Notice  bool = false
	Verbose bool = false
)

func ERROR(format string, v ...interface{}) {
	if Error {
		log.Println(string(debug.Stack()))
		log.SetPrefix("ERROR: ")
		log.Fatalf(fmt.Sprintf(format, v...))
	}
}

func WARNING(format string, v ...interface{}) {
	if Warning {
		log.SetPrefix("WARNING: ")
		log.Println(fmt.Sprintf(format, v...))
	}
}

func NOTICE(format string, v ...interface{}) {
	if Notice {
		log.SetPrefix("NOTICE: ")
		log.Println(fmt.Sprintf(format, v...))
	}
}

func VERBOSE(format string, v ...interface{}) {
	if Verbose {
		log.SetPrefix("VERBOSE: ")
		log.Println(fmt.Sprintf(format, v...))
	}
}

func assert(a bool, format string, v ...interface{}) {
	if Error && !a {
		ERROR(format, v...)
	}
}

func randomElectionTime() time.Duration {
	return time.Duration(rand.Int())%ElectionTimeout + ElectionTimeout
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func shrinkEntriesArray(logs []Log) []Log {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(logs)*lenMultiple < cap(logs) {
		newEntries := make([]Log, len(logs))
		copy(newEntries, logs)
		return newEntries
	}
	return logs
}

func quorumMin(matchIndex []int) int {
	temp := make([]int, len(matchIndex), len(matchIndex))
	copy(temp, matchIndex)
	sort.Ints(temp)
	return temp[(len(temp)-1)/2]
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandTerm  int
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (a ApplyMsg) String() string {
	if a.CommandValid {
		return fmt.Sprintf("{Command:%v,CommandTerm:%v,CommandIndex:%v}", a.Command, a.CommandTerm, a.CommandIndex)
	} else {
		return fmt.Sprintf("{Snapshot:%v,SnapshotTerm:%v,SnapshotIndex:%v}", a.Snapshot, a.SnapshotTerm, a.SnapshotIndex)
	}
}

type EntryType int

const (
	Unknown  EntryType = 0
	Data     EntryType = 1
	Noop     EntryType = 2
	SnapShot EntryType = 3
)

func (e EntryType) String() string {
	switch e {
	case Unknown:
		return "Unknown"
	case Data:
		return "Data"
	case Noop:
		return "Noop"
	case SnapShot:
		return "SnapShot"
	default:
		panic(fmt.Sprintf("Unknown EntryType:%d", e))
	}
}

type Log struct {
	EntryType EntryType
	Command   interface{}
	Term      int
	Index     int
}

func (l Log) String() string {
	return fmt.Sprintf("{EntryType:%v,Command:%v,Term:%v,Index:%v}", l.EntryType, l.Command, l.Term, l.Index)
}

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic(fmt.Sprintf("Unknown Role:%d", r))
	}
}

const (
	ElectionTimeout  = time.Millisecond * 300 // election
	HeartBeatTimeout = time.Millisecond * 150 // send no more than ten times per sec
	ApplyInterval    = time.Millisecond * 100 // apply log
	MaxLockTime      = time.Millisecond * 10  // debug
)

type Err string

const (
	ok      Err = "ok"
	RPCFail Err = "RPCFail"
)

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

func (r RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term:%v,CandidateId:%v,LastLogIndex:%v,LastLogTerm:%v}", r.Term, r.CandidateId, r.LastLogIndex, r.LastLogTerm)
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means that candidate received vote
	Err         Err  // indicate whether successfully delivered
	Server      int  // the RPC's receiver
}

func (r RequestVoteReply) String() string {
	return fmt.Sprintf("{Term:%v,VoteGranted:%v,Err:%v,Server:%v}", r.Term, r.VoteGranted, r.Err, r.Server)
}

type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one)
	LeaderCommit int   // leader's commitIndex
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,PrevLogTerm:%v,Entries:%v,LeaderCommit:%v}", a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.Entries, a.LeaderCommit)
}

type AppendEntriesReply struct {
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	FirstIndex   int  // the first log's index which has the same term as the conflict one
	ConflictTerm int  // the conflict log's term
}

func (a AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v,FirstIndex:%v,ConflictTerm:%v}", a.Term, a.Success, a.FirstIndex, a.ConflictTerm)
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk
}

func (a InstallSnapshotArgs) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,LastIncludedIndex:%v,LastIncludedTerm:%v,Data:%v}", a.Term, a.LeaderId, a.LastIncludedIndex, a.LastIncludedTerm, a.Data)
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (r InstallSnapshotReply) String() string {
	return fmt.Sprintf("{Term:%v}", r.Term)
}