package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"encoding/binary"
	"log"
	"sync"
	"sync/atomic"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

var shardConfigName string = "shardconfig_curr"
var shardConfigNextName string = "shardconfig_next"
var controllerLockName string = "controller_mutex"
var LeaseDuration time.Duration = 2 * time.Second

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed       int32 // set by Kill()
	leases       bool
	leaseVersion rpc.Tversion

	grpsClerks map[tester.Tgid]*shardgrp.Clerk // one clerk for each shard group
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)

	sck.grpsClerks = make(map[tester.Tgid]*shardgrp.Clerk)
	return sck
}

func serializeLease(t time.Time) string {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, t.UnixMilli())
	return string(buf[:n])
}

func parseLease(val string) time.Time {
	buf := []byte(val)
	ms, n := binary.Varint(buf)
	if n <= 0 {
		return time.UnixMilli(0)
	}
	return time.UnixMilli(ms)
}

// ensureGrpClerksFor makes sure sck.grpsClerks has a clerk for every group in cfg.
func (sck *ShardCtrler) ensureGrpClerksFor(cfg *shardcfg.ShardConfig) {
	for gid, servers := range cfg.Groups {
		if _, ok := sck.grpsClerks[gid]; ok {
			continue
		}
		if len(servers) == 0 {
			sck.grpsClerks[gid] = nil
		} else {
			sck.grpsClerks[gid] = shardgrp.MakeClerk(sck.clnt, servers)
		}
	}
}

// Use new config to apply migration among servers
func (sck *ShardCtrler) ApplyMigration(curr, next *shardcfg.ShardConfig, curVersion rpc.Tversion) rpc.Err {
	// Create clerks for all groups mentioned in either config (needed after a crash).
	sck.ensureGrpClerksFor(curr)
	sck.ensureGrpClerksFor(next)

	var wg sync.WaitGroup

	// check all shards
	for i := range len(next.Shards) {
		srcGid := curr.Shards[i]
		dstGid := next.Shards[i]

		// if valid srcGid
		if srcGid != dstGid {
			wg.Add(1)
			go func(shId shardcfg.Tshid, srcGid, dstGid tester.Tgid) {
				defer wg.Done()
				var state []byte
				var srcCk, dstCk *shardgrp.Clerk
				var err rpc.Err

				if srcGid != 0 {
					srcCk = sck.grpsClerks[srcGid]

					for {
						if !sck.isHoldingLock() {
							return
						}
						state, err = srcCk.FreezeShard(shId, curr.Num)
						if err == rpc.OK {
							break
						}
						if err == rpc.ErrWrongGroup {
							state = nil
							break
						}
						if err == rpc.ErrVersion {
							return
						}
						time.Sleep(100 * time.Millisecond)
						// log.Printf("ShardCtrler: fail to freeze shard %v in group %v. Error: %v\n", i, srcGid, err)
					}
				}

				if dstGid != 0 && (state != nil || srcGid == 0) {
					// && state != nil
					dstCk = sck.grpsClerks[dstGid]

					for {
						if !sck.isHoldingLock() {
							return
						}
						err = dstCk.InstallShard(shId, state, next.Num)
						if err == rpc.OK {
							break
						}
						if err == rpc.ErrVersion {
							return
						}
						time.Sleep(50 * time.Millisecond)
						// log.Printf("ShardCtrler: fail to install shard %v in group %v. Error: %v\n", i, dstGid, err)
					}
				}

				if srcGid != 0 {
					// && state != nil
					for {
						if !sck.isHoldingLock() {
							return
						}
						err = srcCk.DeleteShard(shId, next.Num)
						if err == rpc.OK {
							break
						}
						if err == rpc.ErrWrongGroup {
							break
						}
						if err == rpc.ErrVersion {
							return
						}
						time.Sleep(100 * time.Millisecond)
						// log.Printf("ShardCtrler: fail to install shard %v in group %v. Error: %v\n", i, srcGid, err)
					}
				}
			}(shardcfg.Tshid(i), srcGid, dstGid)
		}
	}
	wg.Wait()

	// Drop clerks for groups that no longer exist in the new config.
	for gid := range curr.Groups {
		if _, ok := next.Groups[gid]; !ok {
			delete(sck.grpsClerks, gid)
		}
	}

	// Atomically advance current config from old -> new.
	for {
		err := sck.IKVClerk.Put(shardConfigName, next.String(), curVersion)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrVersion {
			return rpc.ErrVersion
		}
		if err == rpc.ErrMaybe {
			_, v, e := sck.IKVClerk.Get(shardConfigName)
			if e == rpc.OK && v == curVersion+1 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return rpc.OK
}

func (sck *ShardCtrler) AcquireLock() {
	for {
		val, ver, _ := sck.IKVClerk.Get(controllerLockName)
		// 如果 key 不存在或为空，expiry 为 0 时间，必然小于 Now
		expiry := parseLease(val)
		now := time.Now()
		newExpiry := time.UnixMilli(now.Add(LeaseDuration).UnixMilli())
		newVal := serializeLease(newExpiry)

		// 如果租约已过期，尝试获取锁
		if now.After(expiry) {
			// 尝试 CAS 更新
			err := sck.IKVClerk.Put(controllerLockName, newVal, ver)
			if err == rpc.OK {
				sck.leaseVersion = ver + 1
				return
			}
			if err == rpc.ErrMaybe {
				val2, ver2, _ := sck.IKVClerk.Get(controllerLockName)
				if ver2 == ver+1 && val2 == newVal {
					// Acquire lock successfully.
					sck.leaseVersion = ver + 1
					return
				}
			}
			// 如果 Put 失败（版本不匹配），说明锁被别人更新了，继续循环
		}

		// 锁未过期，等待重试
		time.Sleep(50 * time.Millisecond)
	}
}

func (sck *ShardCtrler) isHoldingLock() bool {
	if !sck.leases {
		return true
	}

	val, version, _ := sck.IKVClerk.Get(controllerLockName)
	if version != sck.leaseVersion || time.Now().After(parseLease(val)) {
		return false
	}
	return true
}

func (sck *ShardCtrler) renew() bool {
	val, version, _ := sck.IKVClerk.Get(controllerLockName)

	if version != sck.leaseVersion || time.Now().After(parseLease(val)) {
		// Not a lease holder.
		return false
	}

	for {
		leaseTime := serializeLease(time.Now().Add(LeaseDuration))
		err := sck.IKVClerk.Put(controllerLockName, leaseTime, version)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrMaybe {
			val2, ver2, _ := sck.IKVClerk.Get(controllerLockName)
			if ver2 == version+1 && val2 == leaseTime {
				break
			}
		}
	}

	sck.leaseVersion = version + 1
	return true
}

func (sck *ShardCtrler) startAutoRenew() (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(LeaseDuration / 2)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if !sck.renew() {
					return
				}
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

func (sck *ShardCtrler) ReleaseLock() {
	for {
		_, ver, _ := sck.IKVClerk.Get(controllerLockName)
		now := serializeLease(time.Now())

		// 尝试置空释放
		// 注意：这里用 time.UnixMilli(0) 表示 0 时间，即立即过期
		err := sck.IKVClerk.Put(controllerLockName, now, ver)
		if err == rpc.OK {
			return
		}
		if err == rpc.ErrMaybe {
			val2, ver2, _ := sck.IKVClerk.Get(controllerLockName)
			if ver2 == ver+1 && val2 == now {
				return
			}
		}
		// 如果 Put 失败，可能是发生了变化，循环重试检查
	}
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() rpc.Err {
	sck.leases = true

	// Acquire lock
	sck.AcquireLock()
	defer sck.ReleaseLock()

	// 1. Check current config
	currStr, currVer, _ := sck.IKVClerk.Get(shardConfigName)

	currCfg := shardcfg.FromString(currStr)

	// 2. Check next config
	nextStr, _, _ := sck.IKVClerk.Get(shardConfigNextName)

	nextCfg := shardcfg.FromString(nextStr)

	// 3. Keep going if next > current
	if nextCfg.Num > currCfg.Num {
		return sck.ApplyMigration(currCfg, nextCfg, currVer)
	}

	return rpc.OK
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
	sck.leases = false

	// Initialize the group clerks for each group
	for gid, servers := range cfg.Groups {
		if len(servers) == 0 {
			sck.grpsClerks[gid] = nil // no servers for this shard
		} else {
			sck.grpsClerks[gid] = shardgrp.MakeClerk(sck.clnt, servers)
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) rpc.Err {
	// get cfg
	var old *shardcfg.ShardConfig
	for {
		str, _, err := sck.IKVClerk.Get(shardConfigName)
		if err == rpc.OK {
			old = shardcfg.FromString(str)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// check num
	if new.Num != old.Num+1 {
		// log.Printf("new config's num is less than cfg\n")
		return rpc.ErrVersion
	}

	// Acquire lock
	sck.AcquireLock()
	stop := sck.startAutoRenew()
	defer func() { stop(); sck.ReleaseLock() }()

	newStr := new.String()
	for {
		if !sck.isHoldingLock() {
			return rpc.ErrVersion
		}
		err := sck.IKVClerk.Put(shardConfigNextName, newStr, rpc.Tversion(old.Num))
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrVersion {
			return rpc.ErrVersion
		}
		// Retry saving intent
		if err == rpc.ErrMaybe {
			val, v, gerr := sck.IKVClerk.Get(shardConfigNextName)
			if gerr == rpc.OK {
				if v == rpc.Tversion(old.Num)+1 && newStr == val {
					break
				}
				if v > rpc.Tversion(old.Num)+1 || newStr != val {
					return rpc.ErrVersion
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Ensure we have clerks for any new groups before migration (finishReconfig also ensures).
	if !sck.isHoldingLock() {
		return rpc.ErrVersion
	}
	sck.ensureGrpClerksFor(new)

	return sck.ApplyMigration(old, new, rpc.Tversion(old.Num))
}

// Tester "kills" shardctrler by calling Kill().  For your
// convenience, we also supply isKilled() method to test killed in
// loops.
func (sck *ShardCtrler) Kill() {
	atomic.StoreInt32(&sck.killed, 1)
}

func (sck *ShardCtrler) isKilled() bool {
	z := atomic.LoadInt32(&sck.killed)
	return z == 1
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	for {
		str, _, err := sck.IKVClerk.Get(shardConfigName)
		if err == rpc.OK {
			return shardcfg.FromString(str)
		}
		log.Printf("ShardCtrler: fail to read config.")
		time.Sleep(50 * time.Millisecond)
	}
}
