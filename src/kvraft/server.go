package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	CkName    string
	Timestamp int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data         map[string]string
	maxTimestamp map[string]int64            // 每个clerk完成的最新命令的时间戳
	cache        map[string]map[int64]string // 暂存Get的返回数据  map[ckName]map[timestamp]result
	doneCh       map[string]chan string      // applyMsg应用完成通道
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%v ---- %d 接收 Get 请求，args = %+v\n", time.Now(), kv.me, args)
	op := Op{OpType: "Get", Key: args.Key, CkName: args.CkName, Timestamp: args.Timestamp}

	kv.mu.Lock()
	kv.clearCache(args.CkName, args.Timestamp)
	maxTimestamp := kv.maxTimestamp[args.CkName]
	kv.mu.Unlock()

	if args.Timestamp > maxTimestamp {
		_, _, isLeader := kv.rf.Start(op) // kv.rf.Start(op)是并发安全的
		reply.Retry = !isLeader
		if isLeader {
			chName := fmt.Sprintf("%s-%d", args.CkName, args.Timestamp)
			kv.mu.Lock()
			kv.doneCh[chName] = make(chan string)
			ch := kv.doneCh[chName]
			kv.mu.Unlock()
			select {
			case value := <-ch:
				reply.Value = value
			case <-time.NewTimer(time.Second).C:
				reply.Err = ErrTimeout
				reply.Retry = true
			}
			kv.mu.Lock()
			close(kv.doneCh[chName])
			delete(kv.doneCh, chName)
			kv.mu.Unlock()
		} else {
			reply.Err = Err(fmt.Sprintf("err = %s, current = %d, leader = %d", ErrWrongLeader, kv.me, kv.rf.GetLeader()))
		}
	} else { // 带着结果的rpc返回失败后，重试的rpc进入这里
		kv.mu.Lock()
		if _, exist := kv.cache[args.CkName][args.Timestamp]; exist {
			reply.Value = kv.cache[args.CkName][args.Timestamp]
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%v ---- %d 接收 %s 请求，args = %+v\n", time.Now(), kv.me, args.Op, args)
	op := Op{args.Op, args.Key, args.Value, args.CkName, args.Timestamp}

	kv.mu.Lock()
	kv.clearCache(args.CkName, args.Timestamp)
	maxTimestamp := kv.maxTimestamp[args.CkName]
	kv.mu.Unlock()

	if args.Timestamp > maxTimestamp {
		_, _, isLeader := kv.rf.Start(op)
		reply.Retry = !isLeader
		if isLeader {
			chName := fmt.Sprintf("%s-%d", args.CkName, args.Timestamp)
			kv.mu.Lock()
			kv.doneCh[chName] = make(chan string)
			ch := kv.doneCh[chName]
			kv.mu.Unlock()
			select {
			case <-ch:
			case <-time.NewTimer(time.Second).C:
				reply.Err = ErrTimeout
				reply.Retry = true
			}
			kv.mu.Lock()
			close(kv.doneCh[chName])
			delete(kv.doneCh, chName)
			kv.mu.Unlock()
		} else {
			reply.Err = Err(fmt.Sprintf("err = %s, current = %d, leader = %d", ErrWrongLeader, kv.me, kv.rf.GetLeader()))
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.maxTimestamp = make(map[string]int64)
	kv.cache = make(map[string]map[int64]string)
	kv.doneCh = make(map[string]chan string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	//if err := json.Unmarshal(persister.ReadSnapshot(), &kv.data); err != nil {
	//	log.Fatalf("读取快照失败，错误原因：%s\n", err.Error())
	//}

	go kv.apply()

	return kv
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.SnapshotValid {
			//if err := json.Unmarshal(msg.Snapshot, &kv.data); err != nil {
			//	log.Fatalf("读取快照失败，错误原因：%s\n", err.Error())
			//} else {
			//	kv.mu.Lock()
			//	kv.doneIndex = max(kv.doneIndex, msg.SnapshotIndex)
			//	kv.mu.Unlock()
			//}
		} else if msg.CommandValid {
			kv.doCommand(msg.Command.(Op))
			//if kv.maxraftstate != -1 && msg.CommandIndex%kv.maxraftstate == 0 {
			//	snapshot, err := json.Marshal(kv.data)
			//	if err != nil {
			//		log.Fatalf("序列化数据失败，失败原因：%s\n", err.Error())
			//	} else {
			//		go kv.rf.Snapshot(msg.CommandIndex, snapshot)
			//	}
			//}
		}
	}
}

func (kv *KVServer) doCommand(cmd Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	chName := fmt.Sprintf("%s-%d", cmd.CkName, cmd.Timestamp)
	switch cmd.OpType {
	case "Get":
		if cmd.Timestamp > kv.maxTimestamp[cmd.CkName] {
			kv.clearCache(cmd.CkName, cmd.Timestamp)
			kv.cache[cmd.CkName][cmd.Timestamp] = kv.data[cmd.Key]
			kv.maxTimestamp[cmd.CkName] = cmd.Timestamp
			if ch, exist := kv.doneCh[chName]; exist {
				ch <- kv.data[cmd.Key]
			}
		}
	case "Put":
		if cmd.Timestamp > kv.maxTimestamp[cmd.CkName] {
			kv.data[cmd.Key] = cmd.Value
			kv.maxTimestamp[cmd.CkName] = cmd.Timestamp
			if ch, exist := kv.doneCh[chName]; exist {
				ch <- kv.data[cmd.Key]
			}
		}
	case "Append":
		if cmd.Timestamp > kv.maxTimestamp[cmd.CkName] {
			kv.data[cmd.Key] += cmd.Value
			kv.maxTimestamp[cmd.CkName] = cmd.Timestamp
			if ch, exist := kv.doneCh[chName]; exist {
				ch <- kv.data[cmd.Key]
			}
		}
	}

}

// 清除已完成的Get请求的结果缓存
func (kv *KVServer) clearCache(ckName string, maxTimestamp int64) {
	_, exist := kv.cache[ckName]
	if !exist {
		kv.cache[ckName] = make(map[int64]string)
	}
	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		toDel := make([]int64, 0, len(kv.cache[ckName]))
		for k := range kv.cache[ckName] {
			if k < maxTimestamp {
				toDel = append(toDel, k)
			}
		}
		for _, k := range toDel {
			delete(kv.cache[ckName], k)
		}
	}()
}
