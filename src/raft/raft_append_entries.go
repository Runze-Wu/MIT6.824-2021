package raft

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unLock("AppendEntries")

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.FirstIndex = rf.getLastLogIndex()
	reply.ConflictTerm = -1
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
	rf.persist()
	rf.setElectionTimer(randomElectionTime())

	// For an entry to fit into our log, it must not leave a gap.
	if args.PrevLogIndex > rf.getLastLogIndex() {
		VERBOSE("Rejecting AppendEntries RPC: would leave gap")
		return // response was set to a rejection above
	}

	if args.PrevLogIndex >= rf.getLogStartIndex() &&
		rf.getLogByIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.FirstIndex = args.PrevLogIndex
		reply.ConflictTerm = rf.getLogByIndex(reply.FirstIndex).Term
		rf.printState()
		VERBOSE("Rejecting AppendEntries RPC: terms don't agree %d:%d vs %d:%d",
			reply.FirstIndex, reply.ConflictTerm, args.PrevLogIndex, args.PrevLogTerm)
		for i := reply.FirstIndex - 1; i > rf.commitIndex; i-- {
			if rf.getLogByIndex(i).Term == reply.ConflictTerm {
				reply.FirstIndex--
			}
		}
		return // response was set to a rejection above
	}

	reply.Success = true // we're accepting the request

	// This needs to be able to handle duplicated RPC requests. We compare the
	// entries' terms to know if we need to do the operation; otherwise,
	// reapplying requests can result in data loss.
	index := args.PrevLogIndex
	for i, entry := range args.Entries {
		index++
		assert(entry.Index == index, "index not matched")
		if index < rf.getLogStartIndex() {
			// We already snapshotted and discarded this index, so presumably
			// we've received a committed entry we once already had.
			continue
		}
		if rf.getLastLogIndex() >= index {
			if rf.getLogByIndex(index).Term == entry.Term {
				continue // avoid due to receive prev request then truncate append entry
			}
			assert(rf.commitIndex < index, "should never truncate committed entries")
			rf.log = rf.log[:rf.getIdx(index)]
		}
		rf.log = shrinkEntriesArray(append(rf.log, args.Entries[i:]...))
		rf.persist()
		break
	}

	// Set our committed ID from the request's. In rare cases, this would make
	// our committed ID decrease.
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		assert(rf.commitIndex <= rf.getLastLogIndex(), "append RPC: commitIndex larger than log length")
		rf.printState()
		VERBOSE("New commitIndex server %d: %d", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
	rf.setElectionTimer(randomElectionTime())
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
			rf.lock("ResetHBTimer")
			rf.setHeartBeatTimer()
			if rf.role != Leader {
				return
			}
			rf.unLock("ResetHBTimer")
			go rf.replicate()
		}
	}
}

func (rf *Raft) replicate() {
	rf.lock("Replicate")
	defer rf.unLock("Replicate")
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendHeartBeat(i)
		}
	}
}

func (rf *Raft) genAppendEntriesArgs(server int) *AppendEntriesArgs {
	prevLogIndex := rf.nextIndex[server] - 1
	lastLogIndex := rf.getLastLogIndex()
	assert(lastLogIndex >= prevLogIndex,
		"leader %d send hb msg to server %d, but lastLogIndex %d < prevLogIndex %d", rf.me, server, lastLogIndex, prevLogIndex)
	logs := append([]Log{}, rf.log[rf.getIdx(rf.nextIndex[server]):]...)
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.getLogByIndex(prevLogIndex).Term,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
}

//
// send heartBeat msg to the specified server
//
func (rf *Raft) sendHeartBeat(server int) {
	rf.lock("SendHeartBeat")
	if rf.role != Leader {
		VERBOSE("non-leader server %d, want to send heartbeat", rf.me)
		rf.unLock("SendHeartBeat")
		return
	}
	// Don't have needed entry: send a snapshot instead.
	if rf.nextIndex[server] <= rf.getLogStartIndex() {
		rf.unLock("SendHeartBeat")
		rf.SendSnapshot(server)
		return
	}
	args := rf.genAppendEntriesArgs(server)
	prevLogIndex := args.PrevLogIndex
	VERBOSE("AppendEntries send to server %d %v", server, args)
	rf.unLock("SendHeartBeat")

	reply := &AppendEntriesReply{}
	if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		rf.lock("GetAppendReply")
		defer rf.unLock("GetAppendReply")
		if rf.currentTerm != args.Term {
			// we don't care about result of RPC
			return
		}
		assert(rf.role == Leader, "server %d isn't leader", rf.me)
		if reply.Term > rf.currentTerm {
			NOTICE("Received AppendEntries response from server %d in term %d "+
				"(this server's term was %d)", server, reply.Term, rf.currentTerm)
			rf.stepDown(reply.Term)
			rf.persist()
			return
		}

		if reply.Success {
			if rf.matchIndex[server] > prevLogIndex+len(args.Entries) {
				WARNING("matchIndex should monotonically increase within a term, " +
					"since servers don't forget entries. But it didn't.")
				// stale append RPC could lead to this happen
			} else {
				rf.matchIndex[server] = prevLogIndex + len(args.Entries)
				VERBOSE("AppendEntries success server %d, matchIndex %d", server, rf.matchIndex[server])
				rf.advanceCommitIndex()
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else {
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
			if reply.FirstIndex < rf.nextIndex[server] {
				rf.nextIndex[server] = reply.FirstIndex
			}
			if reply.ConflictTerm != -1 {
				firstIndex := rf.getLogStartIndex()
				for i := args.PrevLogIndex; i >= firstIndex; i-- {
					if rf.getLogByIndex(i).Term == reply.ConflictTerm {
						rf.nextIndex[server] = i + 1
						break
					}
				}
			}
			VERBOSE("decrease server %d's nextIndex to %d", server, rf.nextIndex[server])
		}
	}
	return
}

//
//Move forward #commitIndex if possible. Called only on leaders after
//receiving RPC responses and flushing entries to disk.
//
func (rf *Raft) advanceCommitIndex() {
	assert(rf.role == Leader, "non-leader called advanceCommitIndex")
	newCommitIndex := quorumMin(rf.matchIndex)

	if newCommitIndex <= rf.commitIndex {
		return
	}
	assert(newCommitIndex >= rf.getLogStartIndex(), "we already knew it was committed")
	if rf.getLogByIndex(newCommitIndex).Term != rf.currentTerm {
		// At least one of these entries must also be from the current term to
		// guarantee that no server without them can be elected.
		return
	}
	rf.commitIndex = newCommitIndex
	rf.applyCond.Signal()
	VERBOSE("New commitIndex leader %d: %d", rf.me, rf.commitIndex)
	assert(rf.commitIndex <= rf.getLastLogIndex(), "commitIndex larger than log length")
}
