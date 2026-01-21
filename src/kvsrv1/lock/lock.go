package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	Key string
	ClientId string
	Version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, Key: l, ClientId: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.Key)
        // 尝试获取锁
		if err == rpc.ErrNoKey {
			err := lk.ck.Put(lk.Key, lk.ClientId, 0)
			if err == rpc.OK {
				lk.Version = 1
				//fmt.Printf("Client %s acquire lock\n", lk.ClientId)
				return
			}
		} else if err == rpc.OK && value == "" {
			err := lk.ck.Put(lk.Key, lk.ClientId, version)
			if err == rpc.OK {
				lk.Version = version + 1
				//fmt.Printf("Client %s acquire lock\n", lk.ClientId)
				return
			}
		}

        // 如果获取失败，等待一段时间后重试
		//fmt.Printf("Client %s failed to acquire lock, retrying...\n", lk.ClientId)
        time.Sleep(10 * time.Millisecond)
    }
}

func (lk *Lock) Release() {
	// 检查当前锁是否由自己持有
	for {
		ok := lk.ck.Put(lk.Key, "", lk.Version)
		if ok == rpc.OK {
			return
		}

		//fmt.Printf("Client %s failed to release lock, retrying...\n", lk.ClientId)
		time.Sleep(10 * time.Millisecond)
	}
}
