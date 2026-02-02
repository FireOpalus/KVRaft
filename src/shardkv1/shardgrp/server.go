package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type ShardState int

const (
	ShardStateUnknown ShardState = iota
	ShardStateServing
	ShardStateFrozen
	ShardStateDeleted
)

type ClientPutResult struct {
	id int64
	req any
}

type ShardMeta struct {
	state ShardState
	num shardcfg.Tnum
}

// struct of KV use
type KVData struct {
	Version rpc.Tversion
	Value string
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu sync.Mutex
	
	shardKvMap map[shardcfg.Tshid]map[string]KVData

	clientPutResults map[int64]ClientPutResult

	shardStates map[shardcfg.Tshid]ShardMeta
}

//
func (kv *KVServer) DoGet(req rpc.GetArgs) any {
	// check shard
	shard := shardcfg.Key2Shard(req.Key)
	if kv.shardStates[shard].state != ShardStateServing {
		return rpc.GetReply{Err: rpc.ErrWrongGroup}
	}

	kvdata, ok := kv.shardKvMap[shard][req.Key]
	var res rpc.GetReply
	if ok {
		res = rpc.GetReply{
			Err: rpc.OK,
			Value: kvdata.Value,
			Version: kvdata.Version,
		}
	} else {
		res = rpc.GetReply{
			Err: rpc.ErrNoKey,
		}
	}
	return res
}

func (kv *KVServer) DoPut(req rpc.PutArgs) any {
	// check shard
	shard := shardcfg.Key2Shard(req.Key)
	if kv.shardStates[shard].state != ShardStateServing {
		return rpc.PutReply{Err: rpc.ErrWrongGroup}
	}

	// 确保分片 map 已初始化
	if kv.shardKvMap[shard] == nil {
		kv.shardKvMap[shard] = make(map[string]KVData)
	}

	kvdata, ok := kv.shardKvMap[shard][req.Key]
	var res rpc.PutReply
	if !ok {
		if req.Version == 0 {
			kv.shardKvMap[shard][req.Key] = KVData{1, req.Value}
			res.Err = rpc.OK
		} else {
			res.Err = rpc.ErrNoKey
		}
	} else {
		if req.Version == kvdata.Version {
			kvdata.Version++
			kvdata.Value = req.Value
			kv.shardKvMap[shard][req.Key] = kvdata
			res.Err = rpc.OK
		} else {
			res.Err = rpc.ErrVersion
		}
	}
	return res
}

func (kv *KVServer) DoFreezeShard(req shardrpc.FreezeShardArgs) any {
	// check shard
	s, ok := kv.shardStates[req.Shard]
	if !ok || s.state != ShardStateServing {
		return shardrpc.FreezeShardReply{Err: rpc.ErrWrongGroup}
	}

	s.state = ShardStateFrozen
	kv.shardStates[req.Shard] = s
	
	stateMap := make(map[string]KVData)

	// 直接复制该分片的数据
	if shardData, ok := kv.shardKvMap[req.Shard]; ok {
		for k, v := range shardData {
			stateMap[k] = v
		}
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(stateMap); err != nil {
		log.Printf("KVServer DoFreezeShard encode error: %v", err)
	}

	return shardrpc.FreezeShardReply{
		State: w.Bytes(),
		Num:   req.Num,
		Err:   rpc.OK,
	}
}

func (kv *KVServer) DoInstallShard(req shardrpc.InstallShardArgs) any {
	// check shard
	s, ok := kv.shardStates[req.Shard]
	if !ok || (s.state != ShardStateUnknown && s.state != ShardStateDeleted) {
		return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
	}
	// remember to update shardstate
	s.state = ShardStateServing
	kv.shardStates[req.Shard] = s

	if len(req.State) == 0 {
		return shardrpc.InstallShardReply{Err: rpc.OK}
	}
	r := bytes.NewBuffer(req.State)
	d := labgob.NewDecoder(r)
	var tmp map[string]KVData
	if d.Decode(&tmp) != nil {
		log.Printf("Fail to decode.\n")
	} else {
		kv.shardKvMap[req.Shard] = make(map[string]KVData, len(tmp))
		for k, v := range tmp {
			kv.shardKvMap[req.Shard][k] = v
		}
	}

	return shardrpc.InstallShardReply{Err: rpc.OK}
}

func (kv *KVServer) DoDeleteShard(req shardrpc.DeleteShardArgs) any {
	// check shard
	s, ok := kv.shardStates[req.Shard]
	if !ok || s.state != ShardStateFrozen {
		return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
	}

	// 直接删除该分片的整个 map，如果 key 不存在也是安全的
	delete(kv.shardKvMap, req.Shard)
	
	// 更新状态为 Deleted (可选，视具体状态机逻辑而定，或者直接从 shardStates 移除)
	s.state = ShardStateDeleted
	kv.shardStates[req.Shard] = s

	return shardrpc.DeleteShardReply{Err: rpc.OK}
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var res any

	switch req := req.(type) {
	case rpc.GetArgs:
		res = kv.DoGet(req)

	case rpc.PutArgs:
		res = kv.DoPut(req)

	case shardrpc.FreezeShardArgs:
		res = kv.DoFreezeShard(req)

	case shardrpc.InstallShardArgs:
		res = kv.DoInstallShard(req)

	case shardrpc.DeleteShardArgs:
		res = kv.DoDeleteShard(req)

	default:
		log.Fatalf("Unknown req type!")
	}
	return res
}


func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	// Deep copy
	tmp := make(map[shardcfg.Tshid]map[string]KVData)
	for s, m := range kv.shardKvMap {
		shardCopy := make(map[string]KVData, len(m))
		for k, v := range m {
			shardCopy[k] = v
		}
		tmp[s] = shardCopy
	}
	kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(tmp)
	if err != nil {
		log.Printf("KVServer %v data encode error: %v\n", kv.me, err)
		return nil
	}

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tmp map[shardcfg.Tshid]map[string]KVData
	if d.Decode(&tmp) != nil {
		log.Printf("Fail to decode.\n")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.shardKvMap = tmp
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	req := rpc.GetArgs{Key: args.Key}
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	reply.Err = result.(rpc.GetReply).Err
	reply.Value = result.(rpc.GetReply).Value
	reply.Version = result.(rpc.GetReply).Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := rpc.PutArgs{
		Key: args.Key,
		Value: args.Value,
		Version: args.Version,
	}
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	reply.Err = result.(rpc.PutReply).Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	req := shardrpc.FreezeShardArgs{
		Shard: args.Shard,
		Num: args.Num,
	}
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	reply.Err = result.(shardrpc.FreezeShardReply).Err
	reply.Num = result.(shardrpc.FreezeShardReply).Num
	reply.State = result.(shardrpc.FreezeShardReply).State
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	req := shardrpc.InstallShardArgs{
		Shard: args.Shard,
		State: args.State,
		Num: args.Num,
	}
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	reply.Err = result.(shardrpc.InstallShardReply).Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	req := shardrpc.DeleteShardArgs{
		Shard: args.Shard,
		Num: args.Num,
	}
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	reply.Err = result.(shardrpc.DeleteShardReply).Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}

	// initialize parameters (fix loop)
	kv.shardKvMap = make(map[shardcfg.Tshid]map[string]KVData)
	kv.shardStates = make(map[shardcfg.Tshid]ShardMeta)
	kv.clientPutResults = make(map[int64]ClientPutResult)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// gid1 initialize all shards
	if kv.gid == shardcfg.Gid1 {
		for i := range shardcfg.NShards {
			kv.shardStates[shardcfg.Tshid(i)] = ShardMeta{state: ShardStateServing, num: 0}
			kv.shardKvMap[shardcfg.Tshid(i)] = make(map[string]KVData)
		}
	} else {
		// For other groups, initialize shards as Unknown state
		// They will be set to Serving when they receive the configuration
		for i := range shardcfg.NShards {
			kv.shardStates[shardcfg.Tshid(i)] = ShardMeta{state: ShardStateUnknown, num: 0}
			kv.shardKvMap[shardcfg.Tshid(i)] = make(map[string]KVData)
		}
	}


	return []tester.IService{kv, kv.rsm.Raft()}
}
