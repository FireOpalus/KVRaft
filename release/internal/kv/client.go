package kv

import (
	rpc "release/internal/kvrpc"
	"release/pkg/transport"
	"time"
)

type Clerk struct {
	servers  []*transport.ClientEnd
	leaderId int
}

func MakeClerk(servers []string) *Clerk {
	ck := &Clerk{
		servers: make([]*transport.ClientEnd, len(servers)),
	}
	for i, s := range servers {
		ck.servers[i] = transport.MakeClientEnd(s)
	}
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := rpc.GetArgs{Key: key}
	for {
		serverIdx := ck.leaderId
		reply := rpc.GetReply{}
		ok := ck.servers[serverIdx].Call("KVServer.Get", args, &reply)
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {
			return reply.Value
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key, value string) {
	// Assuming Version 0 for simple KV usage or handle it if needed
	args := rpc.PutArgs{Key: key, Value: value, Version: 0}
	for {
		serverIdx := ck.leaderId
		reply := rpc.PutReply{}
		ok := ck.servers[serverIdx].Call("KVServer.Put", &args, &reply)
		if ok && reply.Err == rpc.OK {
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
