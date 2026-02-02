package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"sync"

	// "time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

var shardConfigName string = "shardconfig"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	mu sync.Mutex
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.mu.Lock()
	defer sck.mu.Unlock()

	err := sck.IKVClerk.Put(shardConfigName, cfg.String(), 0)
	if err != rpc.OK {
		log.Fatalf("ShardCtrler: fail to init config.")
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	sck.mu.Lock()
	defer sck.mu.Unlock()

	// get cfg
	str, version, err := sck.IKVClerk.Get(shardConfigName)
	if err != rpc.OK {
		log.Fatalf("ShardCtrler: fail to read config.")
	}
	cfg := shardcfg.FromString(str)
	
	// check num
	if new.Num <= cfg.Num {
		log.Printf("new config's num is less than cfg\n")
		return
	}

	// check all shards
	for i := range len(new.Shards) {
		srcGid := cfg.Shards[i]
		dstGid := new.Shards[i]

		// if valid srcGid
		if srcGid != dstGid {
			var state []byte
			var srcServers, dstServers []string
			var srcCk, dstCk *shardgrp.Clerk
			var err rpc.Err
			
			if srcGid != 0 {
				srcServers = cfg.Groups[srcGid]
				srcCk = shardgrp.MakeClerk(sck.clnt, srcServers)

				state, err = srcCk.FreezeShard(shardcfg.Tshid(i), cfg.Num)
				if err != rpc.OK {
					log.Printf("ShardCtrler: fail to freeze shard %v in group %v.\n", i, srcGid)
				}
			}

			if dstGid != 0 {
				dstServers = new.Groups[dstGid]
				dstCk = shardgrp.MakeClerk(sck.clnt, dstServers)

				err = dstCk.InstallShard(shardcfg.Tshid(i), state, new.Num)
				if err != rpc.OK {
					log.Printf("ShardCtrler: fail to install shard %v in group %v.\n", i, dstGid)
				}
			}

			if srcGid != 0 {
				err = srcCk.DeleteShard(shardcfg.Tshid(i), cfg.Num)
				if err != rpc.OK {
					log.Printf("ShardCtrler: fail to install shard %v in group %v.\n", i, srcGid)
				}
			}
		}
	}

	// update new config
	err = sck.IKVClerk.Put(shardConfigName, new.String(), version)
	if err != rpc.OK {
		log.Printf("ShardCtrler: fail to update config., err: %v", err)
	}
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	sck.mu.Lock()
	defer sck.mu.Unlock()

	str, _, err := sck.IKVClerk.Get(shardConfigName)
	if err != rpc.OK {
		log.Fatalf("ShardCtrler: fail to read config.")
	}
	return shardcfg.FromString(str)
}

