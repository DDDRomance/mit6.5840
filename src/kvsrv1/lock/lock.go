package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, clientID: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// time.Sleep(time.Second)
	for {
		status, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.OK {
			if status == "Release" {
				Err := lk.ck.Put(lk.lockname, lk.clientID, version)
				if Err == rpc.OK {
					break
				}
			} else if status == lk.clientID {
				break
			}
		} else if err == rpc.ErrNoKey {
			Err := lk.ck.Put(lk.lockname, lk.clientID, 0)
			if Err == rpc.OK {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		status, version, err := lk.ck.Get(lk.lockname)

		if err == rpc.OK && status == lk.clientID {
			ok := lk.ck.Put(lk.lockname, "Release", version)
			if ok == rpc.OK {
				return
			}
		} else if err == rpc.OK && status == "Release" {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}
