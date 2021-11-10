package raft

import (
	"fmt"
	"log"
)

const (
	Error   bool = true
	Notice  bool = false
	Verbose bool = false
)

func ERROR(format string, v ...interface{}) {
	if Error {
		log.SetPrefix("ERROR: ")
		log.Fatalf(fmt.Sprintf(format, v...))
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

type Err string

const (
	ok      Err = "ok"
	RPCFail     = "RPCFail"
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
	PervLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one)
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}
