package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"strconv"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd

	ckId string
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

	ck.ckId = strconv.FormatInt(nrand(), 10)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{Num: num, CkId: ck.ckId, SeqId: seqId, OpType: "Query"}
	return ck.command(args, CommandReply{})
}

func (ck *Clerk) Join(servers map[int][]string) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{Servers: servers, CkId: ck.ckId, SeqId: seqId, OpType: "Join"}
	ck.command(args, CommandReply{})
}

func (ck *Clerk) Leave(gids []int) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{GIDs: gids, CkId: ck.ckId, SeqId: seqId, OpType: "Leave"}
	ck.command(args, CommandReply{})
}

func (ck *Clerk) Move(shard int, gid int) {
	seqId := time.Now().UnixMilli()
	args := CommandArgs{Shard: shard, GID: gid, CkId: ck.ckId, SeqId: seqId, OpType: "Move"}
	ck.command(args, CommandReply{})
}

func (ck *Clerk) command(args CommandArgs, reply CommandReply) Config {
	defer time.Sleep(time.Millisecond)
	for {
		for _, srv := range ck.servers {
			r := reply
			ok := srv.Call("ShardCtrler.Command", &args, &r)
			if ok && r.Err == OK {
				return r.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
