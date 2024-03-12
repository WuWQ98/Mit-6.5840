package kvraft

import (
	"6.5840/labrpc"
	"fmt"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ckName       string
	leaderId     int
	maxTimestamp int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ckName = fmt.Sprintf("%p", &ck)
	DPrintf("--------------------->启动clerk<---------------------\n")
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (res string) {
	// You will have to modify this function.
	timestamp := max64(ck.maxTimestamp+1, time.Now().UnixMilli())
	ck.maxTimestamp = timestamp
	args := GetArgs{key, ck.ckName, timestamp}

	for {
		reply := GetReply{}
		i := ck.leaderId
		DPrintf("发送 Get 请求至 %d，args = %+v\n", i, args)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && !reply.Retry {
			DPrintf("%v --- Get 请求成功，reply = %+v\n", time.Now(), reply)
			ck.leaderId = i
			res = reply.Value
			break
		} else {
			if reply.Err == ErrTimeout {
				DPrintf("Get 超时\n")
				time.Sleep(time.Millisecond * time.Duration(100))
			} else {
				DPrintf("Get 未找到Leader\n")
				ck.leaderId = (i + 1) % len(ck.servers)
			}
		}
	}
	DPrintf("%v ---- Get 请求返回，args = %+v，res = %+v\n", time.Now(), args, res)
	return res
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	timestamp := max64(ck.maxTimestamp+1, time.Now().UnixMilli())
	ck.maxTimestamp = timestamp
	args := PutAppendArgs{key, value, op, ck.ckName, timestamp}

	for {
		reply := PutAppendReply{}
		i := ck.leaderId
		DPrintf("发送 %s 请求至 %d，args = %+v\n", op, i, args)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.Retry {
			DPrintf("%v ---- %s 请求成功，reply = %+v\n", time.Now(), op, reply)
			ck.leaderId = i
			break
		} else {
			if reply.Err == ErrTimeout {
				DPrintf("%s 超时\n", op)
				time.Sleep(time.Millisecond * time.Duration(100))
			} else {
				DPrintf("%s 未找到Leader\n", op)
				ck.leaderId = (i + 1) % len(ck.servers)
			}
		}
	}
	DPrintf("%v ---- %s 请求返回，args = %+v\n", time.Now(), op, args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
