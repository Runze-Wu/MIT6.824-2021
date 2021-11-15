package raft

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.lock("CondInstallSnapshot")
	defer rf.unLock("CondInstallSnapshot")

	if rf.commitIndex >= lastIncludedIndex {
		VERBOSE("Staled snapshot: %d <= %d", lastIncludedIndex, rf.commitIndex)
		return false
	}

	if rf.getLastLogIndex() <= lastIncludedIndex {
		rf.log = make([]Log, 1)
	} else {
		rf.log = shrinkEntriesArray(rf.log[lastIncludedIndex-rf.getSnapshot().Index:])
		rf.log[0].EntryType = SnapShot
	}
	// update latest snapshot info and commit/applied index
	rf.log[0].Index, rf.log[0].Term = lastIncludedIndex, lastIncludedTerm
	rf.commitIndex, rf.lastApplied = rf.log[0].Index, rf.log[0].Index
	VERBOSE("CondInstallSnapshot called")
	rf.printElectionState()
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.lock("Snapshot")
	defer rf.unLock("Snapshot")
	snapshotIndex := rf.getSnapshot().Index
	if index <= snapshotIndex {
		VERBOSE("Staled snapshot: current snapIndex %d, new snapIndex %d", snapshotIndex, index)
		return
	}
	rf.log = shrinkEntriesArray(rf.log[index-snapshotIndex:])
	rf.log[0].EntryType = SnapShot
	VERBOSE("Snapshot called")
	rf.printElectionState()
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
}

//
// InstallSnapshot RPC handler
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")
	defer rf.unLock("InstallSnapshot")

	reply.Term = rf.currentTerm
	if args.Term < reply.Term {
		VERBOSE("Caller(%d) is stale. Our term is %d, theirs is %d",
			args.LeaderId, rf.currentTerm, args.Term)
		return // response was set to a rejection above
	} else if args.Term > reply.Term {
		NOTICE("Received InstallSnapshot request from server %d in term %d "+
			"(this server's term was %d)",
			args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = args.Term
	}
	rf.stepDown(args.Term)
	rf.persist()
	rf.setElectionTimer(randomElectionTime())

	if args.LastIncludedIndex < rf.getLogStartIndex() {
		WARNING("The leader sent us a staled snapshot %d < %d",
			args.LastIncludedIndex, rf.getLogStartIndex())
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) genInstallSnapShotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.getSnapshot().Index,
		LastIncludedTerm:  rf.getSnapshot().Term,
		Data:              rf.persister.ReadSnapshot(),
	}
}

//
// Leader send a snapshot to the server
//
func (rf *Raft) SendSnapshot(server int) {
	rf.lock("SendSnapShot")
	if rf.role != Leader {
		VERBOSE("non-leader server %d, want to send snapshot", rf.me)
		rf.unLock("SendSnapShot")
		return
	}
	args := rf.genInstallSnapShotArgs()
	rf.unLock("SendSnapShot")

	reply := &InstallSnapshotReply{}
	if rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
		rf.lock("GetSnapShotReply")
		defer rf.unLock("GetSnapShotReply")
		if rf.currentTerm != args.Term {
			// we don't care about result of RPC
			return
		}
		assert(rf.role == Leader, "server %d isn't leader", rf.me)
		if reply.Term > rf.currentTerm {
			NOTICE("Received InstallSnapshot response from server %d in term %d "+
				"(this server's term was %d)", server, reply.Term, rf.currentTerm)
			rf.stepDown(reply.Term)
			rf.persist()
			return
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.advanceCommitIndex()
	}
}
