package shardctrler

import (
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead         int32 // set by Kill()
	maxraftstate int

	persister    *raft.Persister
	configs      []Config // indexed by config num
	lastSeqId    map[string]int64
	session      map[string]chan struct{}
	snapshotCond *sync.Cond
	lastApplied  int
}

type Op struct {
	CommandArgs
}

func (sc *ShardCtrler) QueryCfg(num int) Config {
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	op := Op{*args}
	sc.mu.Lock()
	id := sc.lastSeqId[args.CkId]
	sc.mu.Unlock()

	if args.SeqId > id {
		_, _, isLeader := sc.rf.Start(op)
		sc.snapshotCond.Broadcast()
		if isLeader {
			sessionName := fmt.Sprintf("%s-%d", args.CkId, args.SeqId)

			sc.mu.Lock()
			sc.session[sessionName] = make(chan struct{})
			ch := sc.session[sessionName]
			sc.mu.Unlock()

			timer := time.NewTimer(time.Second)
			select {
			case <-ch:
				timer.Stop()
				reply.Err = OK
			case <-timer.C:
				reply.Err = ErrTimeout
			}

			sc.mu.Lock()
			reply.Config = sc.QueryCfg(args.Num)
			if _, exist := sc.session[sessionName]; exist {
				close(sc.session[sessionName])
				delete(sc.session, sessionName)
			}
			sc.mu.Unlock()

		} else {
			reply.Err = ErrWrongLeader
		}
	} else { // 带着结果的rpc返回失败后，重试的rpc进入这里
		if args.SeqId < id {
			log.Println("警告! [", args.OpType, "] 出现意外程序行为", "[args] = ", args, "[lastSeqId] = ", id)
		}
		sc.mu.Lock()
		reply.Config = sc.QueryCfg(args.Num)
		sc.mu.Unlock()
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	sc.mu.Lock()
	atomic.StoreInt32(&sc.dead, 1)
	sc.mu.Unlock()
	sc.snapshotCond.Broadcast()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.persister = persister
	sc.session = make(map[string]chan struct{})
	sc.lastSeqId = make(map[string]int64)
	sc.maxraftstate = 1000
	sc.snapshotCond = sync.NewCond(&sc.mu)

	sc.decSnapshot(sc.persister.ReadSnapshot())

	go sc.apply()

	go sc.makeSnapshot()

	return sc
}

func (sc *ShardCtrler) apply() {
	for !sc.killed() {
		timer := time.NewTimer(time.Second)
		select {
		case msg := <-sc.applyCh:
			sc.mu.Lock()
			if msg.SnapshotValid {
				if msg.Snapshot != nil && len(msg.Snapshot) >= 1 {
					sc.decSnapshot(msg.Snapshot)
				}
			} else if msg.CommandValid {
				sc.doCommand(msg.Command.(Op))
				sc.lastApplied = msg.CommandIndex
				sc.snapshotCond.Broadcast()
			}
			sc.mu.Unlock()
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (sc *ShardCtrler) doCommand(cmd Op) {
	switch cmd.OpType {
	case "Join":
		sc.doJoinCommand(cmd)
	case "Leave":
		sc.doLeaveCommand(cmd)
	case "Move":
		sc.doMoveCommand(cmd)
	case "Query":
		sc.doQueryCommand(cmd)
	}
}

func (sc *ShardCtrler) doJoinCommand(cmd Op) {
	if cmd.SeqId > sc.lastSeqId[cmd.CkId] {
		lastCfg := sc.configs[len(sc.configs)-1]
		groups := make(map[int][]string)
		newGidSet := make(map[int]bool)
		for k, v := range lastCfg.Groups {
			newGidSet[k] = true
			groups[k] = v
		}
		for k, v := range cmd.Servers {
			newGidSet[k] = true
			groups[k] = v
		}
		sc.configs = append(sc.configs, Config{
			Num:    lastCfg.Num + 1,
			Shards: Sharding(lastCfg.Shards, newGidSet),
			Groups: groups,
		})
		sc.lastSeqId[cmd.CkId] = cmd.SeqId
		sessionName := fmt.Sprintf("%s-%d", cmd.CkId, cmd.SeqId)
		if ch, exist := sc.session[sessionName]; exist {
			ch <- struct{}{}
		}
	}
}

func (sc *ShardCtrler) doLeaveCommand(cmd Op) {
	if cmd.SeqId > sc.lastSeqId[cmd.CkId] {
		lastCfg := sc.configs[len(sc.configs)-1]
		groups := make(map[int][]string)
		newGidSet := make(map[int]bool)
		for k, v := range lastCfg.Groups {
			groups[k] = v
			newGidSet[k] = true
		}
		for _, gid := range cmd.GIDs {
			delete(groups, gid)
			delete(newGidSet, gid)
		}
		sc.configs = append(sc.configs, Config{
			Num:    lastCfg.Num + 1,
			Shards: Sharding(lastCfg.Shards, newGidSet),
			Groups: groups,
		})
		sc.lastSeqId[cmd.CkId] = cmd.SeqId
		sessionName := fmt.Sprintf("%s-%d", cmd.CkId, cmd.SeqId)
		if ch, exist := sc.session[sessionName]; exist {
			ch <- struct{}{}
		}
	}
}

func (sc *ShardCtrler) doMoveCommand(cmd Op) {
	if cmd.SeqId > sc.lastSeqId[cmd.CkId] {
		lastCfg := sc.configs[len(sc.configs)-1]
		newShards := [NShards]int{}
		for i, shard := range lastCfg.Shards {
			newShards[i] = shard
		}
		newShards[cmd.Shard] = cmd.GID
		sc.configs = append(sc.configs, Config{
			Num:    lastCfg.Num + 1,
			Shards: newShards,
			Groups: lastCfg.Groups,
		})
		sc.lastSeqId[cmd.CkId] = cmd.SeqId
		sessionName := fmt.Sprintf("%s-%d", cmd.CkId, cmd.SeqId)
		if ch, exist := sc.session[sessionName]; exist {
			ch <- struct{}{}
		}
	}
}

func (sc *ShardCtrler) doQueryCommand(cmd Op) {
	if cmd.SeqId > sc.lastSeqId[cmd.CkId] {
		sc.lastSeqId[cmd.CkId] = cmd.SeqId
		sessionName := fmt.Sprintf("%s-%d", cmd.CkId, cmd.SeqId)
		if ch, exist := sc.session[sessionName]; exist {
			ch <- struct{}{}
		}
	}
}

func (sc *ShardCtrler) decSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var configs []Config
	var lastSeqId map[string]int64
	var lastApplied int
	if d.Decode(&configs) != nil || d.Decode(&lastSeqId) != nil || d.Decode(&lastApplied) != nil {
		DPrintf("%v ---- server：%d 反序列化快照失败\n", time.Now(), sc.me)
	} else {
		sc.configs = configs
		sc.lastSeqId = lastSeqId
		sc.lastApplied = lastApplied
	}
}

func (sc *ShardCtrler) makeSnapshot() {
	for !sc.killed() {
		sc.mu.Lock()
		if !sc.killed() {
			for sc.persister.RaftStateSize() < (sc.maxraftstate/4) && !sc.killed() {
				sc.snapshotCond.Wait()
			}
			if !sc.killed() {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(sc.configs)
				e.Encode(sc.lastSeqId)
				e.Encode(sc.lastApplied)
				snapshot := w.Bytes()
				sc.rf.Snapshot(sc.lastApplied, snapshot)
			}
		}
		sc.mu.Unlock()
	}
}
