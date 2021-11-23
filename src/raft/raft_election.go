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
	rf.printState()
	NOTICE("RequestVote %v", args)
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
			rf.printState()
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = rf.currentTerm == args.Term && rf.voteFor == args.CandidateId
	reply.Err = ok
	reply.Server = rf.me
	rf.persist()
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyCh chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
		reply.Err, reply.Server = RPCFail, server
	}
	replyCh <- reply
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogByIndex(lastLogIndex).Term
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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
	rf.persist()
	rf.printState()
	electionDuration := randomElectionTime()
	rf.setElectionTimer(electionDuration)
	rf.stopHeartBeatTimer()
	timer := time.After(electionDuration) // own timer
	args := rf.genRequestVoteArgs()
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
			rf.lock("GetElectionReply")
			if reply.Err != ok {
				rf.unLock("GetElectionReply")
				go func() {
					time.Sleep(10 * time.Millisecond)
					rf.sendRequestVote(reply.Server, args, replyCh)
				}()
			} else if rf.role != Candidate || rf.currentTerm != args.Term {
				VERBOSE("ignore RPC result")
				// we don't care about result of RPC
				rf.unLock("GetElectionReply")
				return
			} else if reply.VoteGranted {
				voteCount++
				NOTICE("Got vote from server %d for term %d", reply.Server, rf.currentTerm)
				rf.unLock("GetElectionReply")
			} else {
				NOTICE("Vote denied by server %d for term %d", reply.Server, rf.currentTerm)
				if rf.currentTerm < reply.Term {
					NOTICE("Received RequestVote response from server %d in term %d "+
						"(this server's term was %d)", reply.Server, reply.Term, rf.currentTerm)
					rf.stepDown(reply.Term)
					rf.persist()
				}
				rf.unLock("GetElectionReply")
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
	assert(rf.role == Candidate, "non-candidate server %d become leader", rf.me)
	NOTICE("Now leader %d for term %d", rf.me, rf.currentTerm)
	rf.role = Leader
	rf.persist()
	rf.printState()
	rf.stopElectionTimer() // leader no need to election
	rf.setHeartBeatTimer()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	// we don't append NOOP msg because it will affect the log index
	// but personally speaking, I think there is a better way to handle this
	// The NOOP msg can be used to handle 'Phantom Reappearance'

	go rf.heartBeatThreadMain() // periodically send HB msg
	go rf.replicate()           // immediately send NOOP
}
