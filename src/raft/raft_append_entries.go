package raft

//
// Helper for sendHeartBeat() to put the right number of entries into the request.
//
func (rf *Raft) getAppendLogs(server int) (prevLogIndex, prevLogTerm int, res []Log) {
	prevLogIndex = rf.nextIndex[server] - 1
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	if lastLogIndex < prevLogIndex {
		ERROR("leader %d send hb msg to server %d, but lastLogIndex %d < prevLogIndex %d",
			rf.me, server, lastLogIndex, prevLogIndex)
	}
	res = append([]Log{}, rf.log[rf.nextIndex[server]:]...)
	prevLogTerm = rf.getLogByIndex(prevLogIndex).Term
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < reply.Term {
		VERBOSE("Caller(%d) is stale. Our term is %d, theirs is %d",
			args.LeaderId, rf.currentTerm, args.Term)
		return // response was set to a rejection above
	} else if args.Term > reply.Term {
		NOTICE("Received AppendEntries request from server %d in term %d "+
			"(this server's term was %d)",
			args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = args.Term
	}
	rf.stepDown(args.Term)
	rf.setElectionTimer(randomElectionTime())
}

//
// send heartBeat msg to the specified server
//
func (rf *Raft) sendHeartBeat(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		VERBOSE("non-leader server %d, want to send heartbeat", rf.me)
		rf.mu.Unlock()
		return
	}
	prevLogIndex, prevLogTerm, logs := rf.getAppendLogs(server)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PervLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != args.Term {
			// we don't care about result of RPC
			return
		}
		if rf.role != Leader {
			// Since we were leader in this term before, we must still be leader in
			// this term.
			ERROR("server %d isn't leader", rf.me)
		}
		if reply.Term > rf.currentTerm {
			NOTICE("Received AppendEntries response from server %d in term %d "+
				"(this server's term was %d)", server, reply.Term, rf.currentTerm)
			rf.stepDown(reply.Term)
			return
		}

		if reply.Success {

		} else {

		}
	}
	return
}

//
// main thread for leader to send heartBeat msg
//
func (rf *Raft) heartBeatThreadMain() {
	for rf.killed() == false {
		select {
		case <-rf.stopCh:
			return
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			rf.setHeartBeatTimer()
			if rf.role != Leader {
				return
			}
			rf.mu.Unlock()
			go rf.replicate()
		}
	}
}

func (rf *Raft) replicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendHeartBeat(i)
		}
	}
}
