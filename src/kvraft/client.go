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

	ckId     string
	leaderId int
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

	time.Sleep(time.Millisecond)
	ck.ckId = fmt.Sprintf("ck-%d-%d", time.Now().UnixMilli(), nrand())
	DPrintf("--------------------->启动clerk<---------------------\n")
	return ck
}

func (ck *Clerk) Put(key string, value string) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{key, value, "Put", ck.ckId, seqId}
	ck.command(args)
}
func (ck *Clerk) Append(key string, value string) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{key, value, "Append", ck.ckId, seqId}
	ck.command(args)
}

func (ck *Clerk) Get(key string) (res string) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{Key: key, Op: "Get", CkId: ck.ckId, SeqId: seqId}
	return ck.command(args)
}

func (ck *Clerk) command(args CommandArgs) string {
	defer time.Sleep(time.Duration(1) * time.Millisecond)
	for {
		var r CommandReply
		ok := ck.servers[ck.leaderId].Call("KVServer.Command", &args, &r)
		if ok && r.Err == OK {
			return r.Value
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}
