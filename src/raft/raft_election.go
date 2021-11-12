package raft

import "time"

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("RequestVote")
	defer rf.unLock("RequestVote")
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogByIndex(lastLogIndex).Term
	logIsOk := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if args.Term > rf.currentTerm {
		NOTICE("Received RequestVote request from server %d in term %d "+
			"(this server's term was %d)",
			args.CandidateId, rf.currentTerm, args.Term)
		rf.stepDown(args.Term)
	}

	if args.Term == rf.currentTerm {
		if logIsOk && rf.voteFor == -1 {
			NOTICE("Voting for %d in term %d", args.CandidateId, rf.currentTerm)
			rf.stepDown(rf.currentTerm)
			rf.setElectionTimer(randomElectionTime())
			rf.voteFor = args.CandidateId
			rf.printElectionState()
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = rf.currentTerm == args.Term && rf.voteFor == args.CandidateId
	reply.Err = ok
	reply.Server = rf.me
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyCh chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
		reply.Err, reply.Server = RPCFail, server
	}
	replyCh <- reply
}

//
// main thread for follower to detect whether to become candidate
//
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.stopCh:
			return
		case <-rf.electionTimer.C:
			rf.startNewElection()
		}
	}
}

//
// candidate start a new election
//
func (rf *Raft) startNewElection() {
	rf.lock("StartNewElection")
	NOTICE("server %d become candidate", rf.me)
	rf.currentTerm++
	rf.role = Candidate
	rf.voteFor = rf.me
	rf.printElectionState()
	electionDuration := randomElectionTime()
	rf.setElectionTimer(electionDuration)
	rf.stopHeartBeatTimer()
	timer := time.After(electionDuration) // own timer
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogByIndex(lastLogIndex).Term
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.unLock("StartNewElection") // unlock for more concurrency

	voteCount, threshold := 1, len(rf.peers)/2+1
	replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, args, replyCh)
		}
	}

	for voteCount < threshold {
		select {
		case <-rf.stopCh:
			return
		case <-timer:
			return
		case reply := <-replyCh:
			if reply.Err != ok {
				go func() {
					time.Sleep(10 * time.Millisecond)
					rf.sendRequestVote(reply.Server, args, replyCh)
				}()
			} else if reply.VoteGranted {
				voteCount++
				//NOTICE("Got vote from server %d for term %d", reply.Server, rf.currentTerm)
			} else {
				rf.lock("GetVote")
				NOTICE("Vote denied by server %d for term %d", reply.Server, rf.currentTerm)
				if rf.currentTerm < reply.Term {
					NOTICE("Received RequestVote response from server %d in term %d "+
						"(this server's term was %d)", reply.Server, reply.Term, rf.currentTerm)
					rf.stepDown(reply.Term)
				}
				rf.unLock("GetVote")
			}
		}
	}
	NOTICE("got %d vote", voteCount)

	rf.lock("BecomeLeader")
	rf.becomeLeader()
	rf.unLock("BecomeLeader")
}

//
// server change to leader and initialize its state
// append a NOOP entry to its log which will send to other servers
//
func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		ERROR("non-candidate server %d become leader", rf.me)
	}
	NOTICE("Now leader %d for term %d", rf.me, rf.currentTerm)
	rf.role = Leader
	rf.printElectionState()
	rf.stopElectionTimer() // leader no need to election
	rf.setHeartBeatTimer()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	rf.log = append(rf.log, Log{
		EntryType: Noop,
		Command:   nil,
		Term:      rf.currentTerm,
		Index:     rf.nextIndex[rf.me],
	}) // append NOOP msg
	rf.nextIndex[rf.me]++
	go rf.heartBeatThreadMain() // periodically send HB msg
	go rf.replicate()           // immediately send NOOP
}
