package shardkv

import kvrpc "GoDB/rpc/shardkv"

type MemoryKVStateMachine struct {
	Status KVStatus
	KV     map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV:     make(map[string]string),
		Status: Normal,
	}
}

func (mkv *MemoryKVStateMachine) Get(key string) (string, kvrpc.ErrorCode) {
	if value, ok := mkv.KV[key]; ok {
		return value, kvrpc.ErrorCode_OK
	}
	return "", kvrpc.ErrorCode_ERR_NO_KEY
}

func (mkv *MemoryKVStateMachine) Put(key string, value string) kvrpc.ErrorCode {
	mkv.KV[key] = value
	return kvrpc.ErrorCode_OK
}

func (mkv *MemoryKVStateMachine) Append(key string, value string) kvrpc.ErrorCode {
	mkv.KV[key] += value
	return kvrpc.ErrorCode_OK
}

// DeepCopy 解决分片处于迁移状态时，避免多个 Goroutine（如持续处理客户端数据的 applyTask 与生成供别的组拉取的 RPC 协程）共享同一个 Map 的底层指针而导致并发读写 Panic。
func (mkv *MemoryKVStateMachine) deepCopy() *MemoryKVStateMachine {
	newMkv := NewMemoryKVStateMachine()
	newMkv.Status = mkv.Status
	for k, v := range mkv.KV {
		newMkv.KV[k] = v
	}
	return newMkv
}
