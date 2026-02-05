package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"os"
	"strconv"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

var shardConfigName string = "shardconfig_curr"
var shardConfigNextName string = "shardconfig_next"
var controllerLockName string = "controller_mutex"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	lock bool
	id string
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.lock = false
	sck.id = strconv.FormatInt(time.Now().UnixNano() + int64(os.Getpid()) * 1e18, 10)
	return sck
}

// Use new config to apply migration among servers
func (sck *ShardCtrler) ApplyMigration(curr, next *shardcfg.ShardConfig) {
	// check all shards
	for i := range len(next.Shards) {
		srcGid := curr.Shards[i]
		dstGid := next.Shards[i]

		// if valid srcGid
		if srcGid != dstGid {
			var state []byte
			var srcServers, dstServers []string
			var srcCk, dstCk *shardgrp.Clerk
			var err rpc.Err
			
			if srcGid != 0 {
				srcServers = curr.Groups[srcGid]
				srcCk = shardgrp.MakeClerk(sck.clnt, srcServers)

				for {
					state, err = srcCk.FreezeShard(shardcfg.Tshid(i), curr.Num)
					if err == rpc.OK {
						break
					}
					if err == rpc.ErrWrongGroup {
						state = nil
						break
					}
					// log.Printf("ShardCtrler: fail to freeze shard %v in group %v. Error: %v\n", i, srcGid, err)
				}
			}

			if dstGid != 0 && state != nil {
				dstServers = next.Groups[dstGid]
				dstCk = shardgrp.MakeClerk(sck.clnt, dstServers)

				for {
					err = dstCk.InstallShard(shardcfg.Tshid(i), state, next.Num)
					if err == rpc.OK {
						break
					}
					// log.Printf("ShardCtrler: fail to install shard %v in group %v. Error: %v\n", i, dstGid, err)
				}
			}

			if srcGid != 0 && state != nil {
				for {
					err = srcCk.DeleteShard(shardcfg.Tshid(i), curr.Num)
					if err == rpc.OK {
						break
					}
					// log.Printf("ShardCtrler: fail to install shard %v in group %v. Error: %v\n", i, srcGid, err)
				}
			}
		}
	}
}

func (sck *ShardCtrler) AcquireLock() {
	if sck.lock {
		return
	}

	var str string
	var ver rpc.Tversion
	var err rpc.Err
	for !sck.lock {
		str, ver, err = sck.IKVClerk.Get(controllerLockName)
		if err == rpc.OK && str == sck.id {
			sck.lock = true
			return
		} else if err == rpc.ErrNoKey {
			ver = 0
		} else if err != rpc.OK || str != "" {
			continue
		}

		err = sck.IKVClerk.Put(controllerLockName, sck.id, ver)
		if err == rpc.OK {
			sck.lock = true
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (sck *ShardCtrler) ReleaseLock() {
	if !sck.lock {
		return
	}

	var str string
	var ver rpc.Tversion
	var err rpc.Err
	for sck.lock {
		str, ver, err = sck.IKVClerk.Get(controllerLockName)
		if str == "" {
			sck.lock = false
			return
		}
		
		err = sck.IKVClerk.Put(controllerLockName, "", ver)
		if err == rpc.OK {
			sck.lock = false
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	// Acquire lock
	sck.AcquireLock()
	defer sck.ReleaseLock()

	// 1. Check current config
	currStr, currVer, err := sck.IKVClerk.Get(shardConfigName)
	if err != rpc.OK {
		// If accessing kvsrv fails, we can't do much. 
		// But usually InitController relies on kvsrv being up.
		return
	}
	currCfg := shardcfg.FromString(currStr)

	// 2. Check next config
	nextStr, _, err := sck.IKVClerk.Get(shardConfigNextName)
	if err != rpc.OK || nextStr == "" {
		return
	}
	nextCfg := shardcfg.FromString(nextStr)

	// 3. Keep going if next > current
	if nextCfg.Num > currCfg.Num {
		sck.ApplyMigration(currCfg, nextCfg)
		// Update current to match next
		// Note: We use currVer from the read. If it changed in between, 
		// it means another controller might have finished it.
		sck.IKVClerk.Put(shardConfigName, nextCfg.String(), currVer)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here

	err := sck.IKVClerk.Put(shardConfigName, cfg.String(), 0)
	for err != rpc.OK {

		log.Printf("ShardCtrler: fail to init config.")
		err = sck.IKVClerk.Put(shardConfigName, cfg.String(), 0)
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {

	// Acquire lock
	sck.AcquireLock()
	defer sck.ReleaseLock()

	// get cfg
	str, version, err := sck.IKVClerk.Get(shardConfigName)
	if err != rpc.OK {
		log.Printf("ShardCtrler: fail to read config.")
	}
	cfg := shardcfg.FromString(str)
	
	// check num
	if new.Num <= cfg.Num {
		log.Printf("new config's num is less than cfg\n")
		return
	}

	_, nextVer, _ := sck.IKVClerk.Get(shardConfigNextName)
	err = sck.IKVClerk.Put(shardConfigNextName, new.String(), nextVer)
	for err != rpc.OK {
		// Retry saving intent
		_, nextVer, _ = sck.IKVClerk.Get(shardConfigNextName)
		err = sck.IKVClerk.Put(shardConfigNextName, new.String(), nextVer)
	}

	sck.ApplyMigration(cfg, new)

	// update new config
	for {
		err = sck.IKVClerk.Put(shardConfigName, new.String(), version)
		if err == rpc.OK {
			break
		}
		log.Printf("ShardCtrler: fail to update config., err: %v", err)
		str, ver, getErr := sck.IKVClerk.Get(shardConfigName)
		if getErr == rpc.OK {
			c := shardcfg.FromString(str)
			if c.Num >= new.Num {
				break
			}
			version = ver
		}
	}
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	str, _, err := sck.IKVClerk.Get(shardConfigName)
	if err != rpc.OK {
		log.Fatalf("ShardCtrler: fail to read config.")
	}
	return shardcfg.FromString(str)
}

