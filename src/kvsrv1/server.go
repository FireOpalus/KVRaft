package kvsrv

import (
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVData struct {
	Version rpc.Tversion
	Value   string
	Expiry  time.Time
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvmap map[string]KVData
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvmap = make(map[string]KVData)

	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			kv.mu.Lock()
			now := time.Now()
			for k, v := range kv.kvmap {
				if !v.Expiry.IsZero() && now.After(v.Expiry) {
					delete(kv.kvmap, k)
				}
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	kvdata, ok := kv.kvmap[key]

	if ok && !kvdata.Expiry.IsZero() && time.Now().After(kvdata.Expiry) {
		delete(kv.kvmap, key)
		ok = false
	}

	if ok {
		reply.Value = kvdata.Value
		reply.Version = kvdata.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	kvdata, ok := kv.kvmap[key]

	if ok && !kvdata.Expiry.IsZero() && time.Now().After(kvdata.Expiry) {
		delete(kv.kvmap, key)
		ok = false
	}

	var expiry time.Time
	if args.TTLMs > 0 {
		expiry = time.Now().Add(time.Duration(args.TTLMs) * time.Millisecond)
	}

	if !ok {
		if args.Version == 0 {
			kv.kvmap[key] = KVData{1, args.Value, expiry}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if args.Version == kvdata.Version {
			kvdata.Version++
			kvdata.Value = args.Value
			kvdata.Expiry = expiry
			kv.kvmap[key] = kvdata
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
