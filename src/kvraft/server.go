package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	lastApplied    int                        // highest CommandIndex received, e.g. max(rf.lastApplied)
	stateMachine   KVStateMachine             // the stateMachine
	maxraftstate   int                        // snapshot if log grows this big
	clientLastOp   map[int64]OpContext        // used to filter out duplicate requests
	notifyChannels map[int]chan *CommandReply // notify the log with the index has applied
	// Your definitions here.
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	NOTICE("{Server %v} process Command:%v", kv.me, args)
	kv.mu.RLock()
	if args.OpType != OpGet && kv.isDuplicated(args) {
		lastReply := kv.clientLastOp[args.ClientId].Reply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		NOTICE("Duplicated command:%v", args)
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(Op{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	notifyCh := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeOut):
		reply.Err = ErrTimeOut
	}
	go func(index int) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.notifyChannels, index)
	}(index) // remove outdated notify channel to reduce memory footprint
}

func (kv *KVServer) isDuplicated(args *CommandArgs) bool {
	opContext, ok := kv.clientLastOp[args.ClientId]
	return ok && opContext.MaxAppliedId >= args.CommandId
}

func (kv *KVServer) getNotifyChan(index int) chan *CommandReply {
	if _, ok := kv.notifyChannels[index]; !ok {
		kv.notifyChannels[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChannels[index]
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// applier read message from apply ch and apply committed entries and snapshot to stateMachine
func (kv *KVServer) applier() {
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		//NOTICE("{Server %v} applying msg %v", kv.me, applyMsg)
		if applyMsg.CommandValid {
			kv.mu.Lock()
			if applyMsg.CommandIndex <= kv.lastApplied {
				NOTICE("{Server %d} discards staled message, commandIndex %d while lastApplied %d", kv.me, applyMsg.CommandIndex, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = applyMsg.CommandIndex

			reply := &CommandReply{}
			op := applyMsg.Command.(Op)
			if op.OpType != OpGet && kv.isDuplicated(op.CommandArgs) {
				NOTICE("{Server %v} doesn't apply duplicated msg %v", kv.me, op.CommandArgs)
				reply = kv.clientLastOp[op.ClientId].Reply
			} else {
				reply = kv.applyCommand(op.CommandArgs)
				if op.OpType != OpGet {
					kv.clientLastOp[op.ClientId] = OpContext{op.CommandId, reply}
				}
			}

			// notify related channel which log added in the current leader
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && applyMsg.CommandTerm == currentTerm {
				NOTICE("{Leader %v} reply command %v", kv.me, op.CommandArgs)
				ch := kv.getNotifyChan(applyMsg.CommandIndex)
				ch <- reply
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			ERROR("have not implemented")
		} else {
			ERROR("illegal applyMsg:%v", applyMsg)
		}
	}
}

func (kv *KVServer) applyCommand(args *CommandArgs) *CommandReply {
	reply := &CommandReply{}
	switch args.OpType {
	case OpPut:
		reply.Err = kv.stateMachine.KVPut(args.Key, args.Value)
	case OpAppend:
		reply.Err = kv.stateMachine.KVAppend(args.Key, args.Value)
	case OpGet:
		reply.Value, reply.Err = kv.stateMachine.KVGet(args.Key)
	}
	return reply
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		mu:             sync.RWMutex{},
		me:             me,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		dead:           0,
		lastApplied:    0,
		stateMachine:   &KVMemory{KVmap: make(map[string]string)},
		maxraftstate:   maxraftstate,
		clientLastOp:   make(map[int64]OpContext),
		notifyChannels: make(map[int]chan *CommandReply),
	}
	// You may need initialization code here.
	go kv.applier()
	return kv
}
