package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandArgs
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32
	persister   *raft.Persister
	ctrler      *shardctrler.Clerk
	db          map[string]string
	lastSeqId   map[string]int64
	session     map[string]chan struct{}
	lastApplied int
}

func (kv *ShardKV) Command(args CommandArgs, reply CommandReply) {

	cfg := kv.ctrler.Query(-1)
	if gid := cfg.Shards[args.Shard]; gid != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.db = make(map[string]string)
	kv.lastSeqId = make(map[string]int64)
	kv.session = make(map[string]chan struct{})

	kv.persister = persister
	// Use something like this to talk to the shardctrler:
	kv.ctrler = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.decSnapshot(kv.persister.ReadSnapshot())

	go kv.apply()

	return kv
}

func (kv *ShardKV) apply() {
	for !kv.killed() || len(kv.applyCh) != 0 {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if msg.SnapshotValid {
				if msg.Snapshot != nil && len(msg.Snapshot) >= 1 {
					kv.decSnapshot(msg.Snapshot)
				}
			} else if msg.CommandValid {
				kv.doCommand(msg.Command.(Op))
				kv.lastApplied = msg.CommandIndex
			}
			kv.mu.Unlock()
			kv.trySnapshot()
		case <-time.NewTimer(time.Millisecond * time.Duration(500)).C:
		}
	}
}

func (kv *ShardKV) doCommand(cmd Op) {
	sessionName := fmt.Sprintf("%s-%d", cmd.CkId, cmd.SeqId)
	switch cmd.Op {
	case "Get":
		if cmd.SeqId > kv.lastSeqId[cmd.CkId] {
			kv.lastSeqId[cmd.CkId] = cmd.SeqId
			if ch, exist := kv.session[sessionName]; exist {
				ch <- struct{}{}
			}
		}
	case "Put":
		if cmd.SeqId > kv.lastSeqId[cmd.CkId] {
			kv.db[cmd.Key] = cmd.Value
			kv.lastSeqId[cmd.CkId] = cmd.SeqId
			if ch, exist := kv.session[sessionName]; exist {
				ch <- struct{}{}
			}
		}
	case "Append":
		if cmd.SeqId > kv.lastSeqId[cmd.CkId] {
			kv.db[cmd.Key] += cmd.Value
			kv.lastSeqId[cmd.CkId] = cmd.SeqId
			if ch, exist := kv.session[sessionName]; exist {
				ch <- struct{}{}
			}
		}
	}
}

func (kv *ShardKV) decSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastSeqId map[string]int64
	var lastApplied int
	if d.Decode(&db) != nil || d.Decode(&lastSeqId) != nil || d.Decode(&lastApplied) != nil {
		log.Printf("shardkv, server：%d 反序列化快照失败\n", kv.me)
	} else {
		if lastApplied > kv.lastApplied {
			kv.db = db
			kv.lastSeqId = lastSeqId
			kv.lastApplied = lastApplied
		}
	}
}

func (kv *ShardKV) trySnapshot() {
	kv.mu.Lock()
	if kv.maxraftstate >= 0 && !kv.killed() && kv.persister.RaftStateSize() >= (kv.maxraftstate/4) {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.db)
		e.Encode(kv.lastSeqId)
		e.Encode(kv.lastApplied)
		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.lastApplied, snapshot)
	}
	kv.mu.Unlock()
}
