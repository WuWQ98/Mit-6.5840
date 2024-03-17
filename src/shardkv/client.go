package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.5840/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	ckId string
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	time.Sleep(time.Millisecond)
	ck.ckId = fmt.Sprintf("ck-%d-%d", time.Now().UnixMilli(), nrand())
	return ck
}

func (ck *Clerk) Put(key string, value string) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{
		Key:   key,
		Value: value,
		Op:    "Put",
		CkId:  ck.ckId,
		SeqId: seqId,
	}
	ck.command(args)
}
func (ck *Clerk) Append(key string, value string) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{
		Key:   key,
		Value: value,
		Op:    "Append",
		CkId:  ck.ckId,
		SeqId: seqId,
	}
	ck.command(args)
}

func (ck *Clerk) Get(key string) (res string) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{
		Key:   key,
		Op:    "Get",
		CkId:  ck.ckId,
		SeqId: seqId,
	}
	return ck.command(args)
}

func (ck *Clerk) command(args CommandArgs) string {
	defer time.Sleep(time.Duration(1) * time.Millisecond)
	shard := key2shard(args.Key)
	args.Shard = shard
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply CommandReply
				ok := srv.Call("ShardKV.Command", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
