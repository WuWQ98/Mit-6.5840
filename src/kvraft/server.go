package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	CommandArgs
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister   *raft.Persister
	db          map[string]string
	lastSeqId   map[string]int64
	session     map[string]chan struct{}
	lastApplied int
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	if kv.killed() {
		return
	}

	op := Op{*args}

	kv.mu.Lock()
	id := kv.lastSeqId[args.CkId]
	kv.mu.Unlock()

	if args.SeqId > id {
		_, _, isLeader := kv.rf.Start(op)
		kv.trySnapshot()
		if isLeader {
			sessionName := fmt.Sprintf("%s-%d", args.CkId, args.SeqId)
			kv.mu.Lock()
			kv.session[sessionName] = make(chan struct{})
			ch := kv.session[sessionName]
			kv.mu.Unlock()

			select {
			case <-ch:
				reply.Err = OK
			case <-time.NewTimer(time.Millisecond * time.Duration(500)).C:
				reply.Err = ErrTimeout
			}

			kv.mu.Lock()
			reply.Value = kv.db[args.Key]
			close(kv.session[sessionName])
			delete(kv.session, sessionName)
			kv.mu.Unlock()

		} else {
			reply.Err = ErrWrongLeader
		}
	} else { // 带着结果的rpc返回失败后，重试的rpc进入这里
		if args.SeqId < id {
			log.Println("警告! [", args.Op, "] 出现意外程序行为", "[server] = ", kv.me, "[killed] = ", kv.killed(), kv.rf.Killed(), "[leader] = ", kv.rf.GetLeader(), "[args] = ", args, "[lastSeqId] = ", id, "[db] = ", kv.db)
		}
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
		reply.Err = OK
	}
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.db = make(map[string]string)
	kv.lastSeqId = make(map[string]int64)
	kv.session = make(map[string]chan struct{})

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.decSnapshot(kv.persister.ReadSnapshot())

	go kv.apply()

	return kv
}

func (kv *KVServer) apply() {
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

func (kv *KVServer) doCommand(cmd Op) {
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

func (kv *KVServer) decSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastSeqId map[string]int64
	var lastApplied int
	if d.Decode(&db) != nil || d.Decode(&lastSeqId) != nil || d.Decode(&lastApplied) != nil {
		log.Printf("kvraft, server：%d 反序列化快照失败\n", kv.me)
	} else {
		if lastApplied > kv.lastApplied {
			kv.db = db
			kv.lastSeqId = lastSeqId
			kv.lastApplied = lastApplied
		}
	}
}

func (kv *KVServer) trySnapshot() {
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
