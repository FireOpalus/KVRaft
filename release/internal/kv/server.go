package kv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"release/internal/kv/rsm"
	rpc "release/internal/kvrpc"
	"release/pkg/labgob"
	"release/pkg/raftapi"
	"release/pkg/storage"
	"release/pkg/transport"
)

// struct of KV use
type KVData struct {
	Version rpc.Tversion
	Value   string
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu sync.Mutex

	kvmap map[string]KVData
}

//

// Req struct

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		kvdata, ok := kv.kvmap[req.Key]
		var res rpc.GetReply
		if ok {
			res = rpc.GetReply{
				Err:     rpc.OK,
				Value:   kvdata.Value,
				Version: kvdata.Version,
			}
		} else {
			res = rpc.GetReply{
				Err: rpc.ErrNoKey,
			}
		}
		return res

	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		kvdata, ok := kv.kvmap[req.Key]
		var res rpc.PutReply
		if !ok {
			if req.Version == 0 {
				kv.kvmap[req.Key] = KVData{1, req.Value}
				res.Err = rpc.OK
			} else {
				res.Err = rpc.ErrNoKey
			}
		} else {
			if req.Version == 0 || req.Version == kvdata.Version {
				kvdata.Version++
				kvdata.Value = req.Value
				kv.kvmap[req.Key] = kvdata
				res.Err = rpc.OK
			} else {
				res.Err = rpc.ErrVersion
			}
		}
		return res

	default:
		log.Fatalf("Unknown req type!")
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	tmp := make(map[string]KVData, len(kv.kvmap))
	for k, v := range kv.kvmap {
		tmp[k] = v
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
	// Your code here
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tmp map[string]KVData
	if d.Decode(&tmp) != nil {
		log.Printf("Fail to decode.\n")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.kvmap = make(map[string]KVData, len(tmp))
		for k, v := range tmp {
			kv.kvmap[k] = v
		}
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) error {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	req := rpc.GetArgs{Key: args.Key}
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return nil
	}
	reply.Err = result.(rpc.GetReply).Err
	reply.Value = result.(rpc.GetReply).Value
	reply.Version = result.(rpc.GetReply).Version
	return nil
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) error {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := rpc.PutArgs{
		Key:     args.Key,
		Value:   args.Value,
		Version: args.Version,
	}
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return nil
	}
	reply.Err = result.(rpc.PutReply).Err
	return nil
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*transport.ClientEnd, me int, persister *storage.FilePersister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	// must inistilize map before rsm
	// because rsm has reader goroutine, this cause wrong number of puts
	kv.kvmap = make(map[string]KVData)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return kv
}

func (kv *KVServer) Raft() raftapi.Raft {
	return kv.rsm.Raft()
}
