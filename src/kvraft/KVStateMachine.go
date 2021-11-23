package kvraft

type KVStateMachine interface {
	KVPut(key, value string) Err
	KVAppend(key, value string) Err
	KVGet(key string) (string, Err)
}

type KVMemory struct {
	KVmap map[string]string
}

func (kv *KVMemory) KVPut(key, value string) Err {
	kv.KVmap[key] = value
	return OK
}

func (kv *KVMemory) KVAppend(key, value string) Err {
	if val, ok := kv.KVmap[key]; ok {
		value = val + value
	}
	kv.KVmap[key] = value
	return OK
}

func (kv *KVMemory) KVGet(key string) (string, Err) {
	if val, ok := kv.KVmap[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}
