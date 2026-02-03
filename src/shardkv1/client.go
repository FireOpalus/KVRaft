package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
	grpClerks map[tester.Tgid]*shardgrp.Clerk
	cfg	*shardcfg.ShardConfig
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
		grpClerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	// You'll have to add code here.
	cfg := sck.Query()
	ck.cfg = cfg
	for gid, servers := range cfg.Groups {
		if len(servers) == 0 {
			ck.grpClerks[gid] = nil
		} else {
			ck.grpClerks[gid] = shardgrp.MakeClerk(clnt, servers)
		}
	}

	return ck
}


// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		shard := shardcfg.Key2Shard(key)
		gid := ck.cfg.Shards[shard]
		grpClerk := ck.grpClerks[gid]

		// clerk not found, update config
		if grpClerk == nil {
			ck.UpdateConfig()
		}

		value, version, err := grpClerk.Get(key, ck.cfg.Num)

		if err == rpc.ErrWrongGroup {
			ck.UpdateConfig()
			time.Sleep(25 * time.Millisecond)
			continue
		}
		log.Printf("9")
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		shard := shardcfg.Key2Shard(key)
		gid := ck.cfg.Shards[shard]
		grpClerk := ck.grpClerks[gid]

		// clerk not found, update config
		if grpClerk == nil {
			ck.UpdateConfig()
		}

		// All shards are assigned to invalid groups, return an error to allow graceful exit
		if gid == 0 || len(ck.cfg.Groups[gid]) == 0 {
			return rpc.ErrNoKey
		}

		err := grpClerk.Put(key, value, version, ck.cfg.Num)

		if err == rpc.ErrWrongGroup {
			ck.UpdateConfig()
			time.Sleep(25 * time.Millisecond)
			continue
		}

		if err == rpc.ErrVersion {
			return rpc.ErrVersion
		}

		return err
	}
	
}

func (ck *Clerk) UpdateConfig() {
	new := ck.sck.Query()
	ck.cfg = new
	// make clerks from new config
	ck.grpClerks = make(map[tester.Tgid]*shardgrp.Clerk)
	// cfg.groups
	for gid, servers := range new.Groups {
		if len(servers) == 0 {
			ck.grpClerks[gid] = nil
		} else {
			ck.grpClerks[gid] = shardgrp.MakeClerk(ck.clnt, servers)
		}
	}
}