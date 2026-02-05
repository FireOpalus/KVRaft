package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"strconv"
	"fmt"
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
	leaseExpiry time.Time
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

func serializeLease(t time.Time) string {
	return fmt.Sprintf("%d", t.UnixMilli())
}

func parseLease(val string) time.Time {
	if val == "" {
		return time.UnixMilli(0)
	}
	ms, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.UnixMilli(0)
	}
	return time.UnixMilli(ms)
}

// Use new config to apply migration among servers
func (sck *ShardCtrler) ApplyMigration(curr, next *shardcfg.ShardConfig) bool {
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
					if time.Now().After(sck.leaseExpiry) {
						return false
					}
					state, err = srcCk.FreezeShard(shardcfg.Tshid(i), curr.Num)
					if err == rpc.OK {
						break
					}
					if err == rpc.ErrWrongGroup {
						state = nil
						break
					}
					time.Sleep(100 * time.Millisecond)
					// log.Printf("ShardCtrler: fail to freeze shard %v in group %v. Error: %v\n", i, srcGid, err)
				}
			}

			if dstGid != 0 && state != nil {
				dstServers = next.Groups[dstGid]
				dstCk = shardgrp.MakeClerk(sck.clnt, dstServers)

				for {
					if time.Now().After(sck.leaseExpiry) {
						return false
					}
					err = dstCk.InstallShard(shardcfg.Tshid(i), state, next.Num)
					if err == rpc.OK {
						break
					}
					time.Sleep(100 * time.Millisecond)
					// log.Printf("ShardCtrler: fail to install shard %v in group %v. Error: %v\n", i, dstGid, err)
				}
			}

			if srcGid != 0 && state != nil {
				for {
					if time.Now().After(sck.leaseExpiry) {
						return false
					}
					err = srcCk.DeleteShard(shardcfg.Tshid(i), curr.Num)
					if err == rpc.OK {
						break
					}
					time.Sleep(100 * time.Millisecond)
					// log.Printf("ShardCtrler: fail to install shard %v in group %v. Error: %v\n", i, srcGid, err)
				}
			}
		}
	}
	return true
}

func (sck *ShardCtrler) AcquireLock() {
	for {
		val, ver, err := sck.IKVClerk.Get(controllerLockName)
		// 如果 key 不存在或为空，expiry 为 0 时间，必然小于 Now
		expiry := parseLease(val)
		now := time.Now()

		// 如果租约已过期，尝试获取锁
		if now.After(expiry) {
			// 设置 1.0 秒的租约时间
			newExpiry := time.UnixMilli(now.Add(1000 * time.Millisecond).UnixMilli())
			newVal := serializeLease(newExpiry)
			
			// 尝试 CAS 更新
			err = sck.IKVClerk.Put(controllerLockName, newVal, ver)
			if err == rpc.OK {
				sck.leaseExpiry = newExpiry
				return
			}
			// 如果 Put 失败（版本不匹配），说明锁被别人更新了，继续循环
		} else {
			// 锁未过期，等待重试
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (sck *ShardCtrler) ReleaseLock() {
	for {
		val, ver, err := sck.IKVClerk.Get(controllerLockName) 
		if err != rpc.OK && err != rpc.ErrNoKey {
			return // 无法访问存储，放弃释放
		}

		// 检查锁是否还是自己的
		currentExpiry := parseLease(val)
		if currentExpiry.UnixMilli() != sck.leaseExpiry.UnixMilli() {
			return // 锁已经不是自己的了（可能过期被别人抢了），不需要释放
		}

		// 尝试置空释放
		// 注意：这里用 time.UnixMilli(0) 表示 0 时间，即立即过期
		emptyVal := serializeLease(time.UnixMilli(0))
		err = sck.IKVClerk.Put(controllerLockName, emptyVal, ver)
		if err == rpc.OK {
			return
		}
		// 如果 Put 失败，可能是发生了变化，循环重试检查
		time.Sleep(10 * time.Millisecond)
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
		if !sck.ApplyMigration(currCfg, nextCfg) {
			return
		}
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
	sck.IKVClerk.Put(shardConfigName, cfg.String(), 0)
	sck.IKVClerk.Put(shardConfigNextName, cfg.String(), 0)
	sck.IKVClerk.Put(controllerLockName, serializeLease(time.Now()), 0)

	
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
		if time.Now().After(sck.leaseExpiry) {
			return
		}
		// Retry saving intent
		_, nextVer, _ = sck.IKVClerk.Get(shardConfigNextName)
		err = sck.IKVClerk.Put(shardConfigNextName, new.String(), nextVer)
		time.Sleep(10 * time.Millisecond)
	}

	if !sck.ApplyMigration(cfg, new) {
		return
	}

	// update new config
	for {
		if time.Now().After(sck.leaseExpiry) {
			return
		}
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
		time.Sleep(10 * time.Millisecond)
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

