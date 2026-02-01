package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	idx := ck.leaderId
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			// if wrong leader error happened, turn to next server
			idx = (idx + 1) % len(ck.servers)
		} else {
			ck.leaderId = idx
			return reply.Value, reply.Version, reply.Err
		}

		// wait if we have tried all servers
		if idx == ck.leaderId {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	firstCall := true
	idx := ck.leaderId
	for {
		args := rpc.PutArgs{
			Key: key,
			Value: value,
			Version: version,
		}
		reply := rpc.PutReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Put", &args, &reply)

		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				idx = (idx + 1) % len(ck.servers)
				// continue to allow other errors to be handled or to sleep if cycled
			} else if reply.Err == rpc.ErrVersion && !firstCall {
				ck.leaderId = idx
				return rpc.ErrMaybe
			} else {
				ck.leaderId = idx
            	return reply.Err
			}
		} else {
			// update idx on RPC failure
			idx = (idx + 1) % len(ck.servers)
		}

		firstCall = false
		
		// wait if we have tried all servers
		if idx == ck.leaderId {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	idx := ck.leaderId
	for {
		args := shardrpc.FreezeShardArgs{
			Shard: s,
			Num: num,
		}
		reply := shardrpc.FreezeShardReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.FreezeShard", &args, &reply)

		if ok {
			switch reply.Err {
			case rpc.ErrWrongLeader:
				idx = (idx + 1) % len(ck.servers)
			case rpc.ErrWrongGroup:
				ck.leaderId = idx
				return nil, reply.Err
			default:
				ck.leaderId = idx
				return reply.State, reply.Err
			}
		} else {
			idx = (idx + 1) % len(ck.servers)
		}

		if idx == ck.leaderId {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	idx := ck.leaderId
	for {
		args := shardrpc.InstallShardArgs{
			Shard: s,
			State: state,
			Num: num,
		}
		reply := shardrpc.InstallShardReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.InstallShard", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			idx = (idx + 1) % len(ck.servers)
		} else {
			return reply.Err
		}

		if idx == ck.leaderId {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	idx := ck.leaderId
	for {
		args := shardrpc.DeleteShardArgs{
			Shard: s,
			Num: num,
		}
		reply := shardrpc.DeleteShardReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.DeleteShard", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			idx = (idx + 1) % len(ck.servers)
		} else {
			return reply.Err
		}

		if idx == ck.leaderId {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
