package shardgrp

import (
	"log"
	"time"
	// "log"

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

func (ck *Clerk) Get(key string, num shardcfg.Tnum) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	idx := ck.leaderId
	args := rpc.GetArgs{Key: key}

	for {	
		reply := rpc.GetReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				ck.leaderId = idx
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrNoKey:
				ck.leaderId = idx
				return "", 0, rpc.ErrNoKey
			case rpc.ErrWrongLeader:
				idx = (idx + 1) % len(ck.servers)
				continue
			case rpc.ErrWrongGroup:
				return "", 0, rpc.ErrWrongGroup
			default:
				log.Fatalf("Get does not have this err!")
			}
		}

		// wait if we have tried all servers
		time.Sleep(10 * time.Millisecond)
		idx = (idx + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion, num shardcfg.Tnum) rpc.Err {
	// Your code here
	firstCall := true
	idx := ck.leaderId
	args := rpc.PutArgs{
			Key: key,
			Value: value,
			Version: version,
		}

	for {
		reply := rpc.PutReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Put", &args, &reply)

		if ok {
			switch reply.Err {
			case rpc.OK:
				ck.leaderId = idx
				return rpc.OK
			case rpc.ErrNoKey:
				ck.leaderId = idx
				return rpc.ErrNoKey
			case rpc.ErrVersion:
				if firstCall {
					ck.leaderId = idx
					return rpc.ErrVersion
				} else {
					return rpc.ErrMaybe
				}
			case rpc.ErrWrongLeader:
				idx = (idx + 1) % len(ck.servers)
				continue
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			case rpc.ErrOutDated:
				return rpc.ErrMaybe
			}
		}

		firstCall = false
		
		// wait if we have tried all servers
		time.Sleep(10 * time.Millisecond)
		idx = (idx + 1) % len(ck.servers)
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
		} else if reply.Err == rpc.ErrWrongGroup {
			ck.leaderId = idx
			// log.Printf("1:%v", reply.Err)
			return reply.Err
		} else {
			// log.Printf("2:%v", reply.Err)
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
if reply.Err == rpc.ErrWrongGroup {
			ck.leaderId = idx
			return reply.Err
		} else 
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
