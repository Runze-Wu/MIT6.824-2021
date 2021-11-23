package kvraft

import (
	"fmt"
	"log"
	"runtime/debug"
	"time"
)

const (
	Error   bool = true
	Warning bool = true
	Notice  bool = false
)

const ExecuteTimeOut = 500 * time.Millisecond

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
		log.SetPrefix("SERVICE: ")
		log.Println(fmt.Sprintf(format, v...))
	}
}

func assert(a bool, format string, v ...interface{}) {
	if Error && !a {
		ERROR(format, v...)
	}
}

type Op struct {
	*CommandArgs
}

type OpContext struct {
	MaxAppliedId int64         // store the latest commandId processed for the client
	Reply        *CommandReply // along with the associated reply
}

type Err uint8

const (
	OK             Err = 0
	ErrNoKey       Err = 1
	ErrWrongLeader Err = 2
	ErrTimeOut     Err = 3
)

func (e Err) String() string {
	switch e {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	default:
		panic(fmt.Sprintf("unexpected Err %d", e))
	}
}

type OpType uint8

const (
	OpPut    OpType = 0
	OpAppend OpType = 1
	OpGet    OpType = 2
)

func (op OpType) String() string {
	switch op {
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	case OpGet:
		return "OpGet"
	default:
		panic(fmt.Sprintf("unexpected OpType %d", op))
	}
}

type CommandArgs struct {
	Key       string
	Value     string
	OpType    OpType // "OpPut" or "OpAppend" or "OpGet"
	ClientId  int64
	CommandId int64
}

func (args CommandArgs) String() string {
	return fmt.Sprintf("{Key:%v,Value:%v,OpType:%v,ClientId:%v,CommandId:%v}", args.Key, args.Value, args.OpType, args.ClientId, args.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (reply CommandReply) String() string {
	return fmt.Sprintf("{Err:%v,Value:%v}", reply.Err, reply.Value)
}
