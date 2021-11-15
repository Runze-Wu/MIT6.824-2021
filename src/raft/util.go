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
	Warning bool = true
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

type Err string

const (
	ok      Err = "ok"
	RPCFail Err = "RPCFail"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means that candidate received vote
	Err         Err  // indicate whether successfully delivered
	Server      int  // the RPC's receiver
}

type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one)
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	FirstIndex   int  // the first log's index which has the same term as the conflict one
	ConflictTerm int  // the conflict log's term
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}
