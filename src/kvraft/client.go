package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int   // the leader of servers
	clientId  int64 // its own unique id
	commandId int64 // <clientId, commandId> uniquely identifies a command
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 0,
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandArgs{Key: key, OpType: OpGet})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, OpType: OpPut})
}

func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, OpType: OpAppend})
}

//
// shared by OpGet, OpPut and OpAppend.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Command", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Command(args *CommandArgs) string {
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId
	for {
		reply := &CommandReply{}
		if !ck.servers[ck.leaderId].Call("KVServer.Command", args, reply) ||
			reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		NOTICE("{Client %v} commandId %v get reply from {Server %d}", ck.clientId, args.CommandId, ck.leaderId)
		ck.commandId++
		return reply.Value
	}
}
