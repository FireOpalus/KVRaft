package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	idx := ck.leaderId
	for {
		args := rpc.GetArgs{}
		args.Key = key
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

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	firstCall := true
	idx := ck.leaderId
	for {
		args := rpc.PutArgs{}
		args.Key = key
		args.Value = value
		args.Version = version
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
