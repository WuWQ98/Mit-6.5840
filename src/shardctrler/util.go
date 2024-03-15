package shardctrler

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

// Debugging
const Debug = false

var filename string

var lock sync.Mutex

func init() {
	filename = fmt.Sprintf("shardctrler_%s.log", time.Now().Format("2006-01-02_15:04:05"))
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if !Debug {
		return
	}
	lock.Lock()
	defer lock.Unlock()
	file, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
	defer file.Close()
	fmt.Fprintf(file, format, a...)
	return
}

func HashTo32(input int) uint32 {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d", input)))
	return binary.BigEndian.Uint32(h[:4]) % math.MaxInt32
}

func ConsistentHashing(shards []int, gids []int, vNodeNum int) []int {
	hashShardIdxs := make([]uint32, len(shards))
	for i := range shards {
		hashShardIdxs[i] = HashTo32(int(HashTo32(int(HashTo32(i)))))
	}
	hashGids := make([]uint32, len(gids)*vNodeNum)
	set := make(map[uint32]int)
	for i, gid := range gids {
		g := gid
		n := vNodeNum
		for n > 0 {
			hashGid := HashTo32(int(HashTo32(int(HashTo32(g)))))
			if _, ok := set[hashGid]; !ok {
				hashGids[i+vNodeNum-n] = hashGid
				set[hashGid] = gid
				n--
				continue
			}
			g++
		}
	}
	sort.Sort((ByUint32)(hashGids))
	for i, hashShardIdx := range hashShardIdxs {
		if hashShardIdx > hashGids[len(hashGids)-1] {
			shards[i] = set[hashGids[0]]
		} else {
			j := 0
			for hashShardIdx > hashGids[j] {
				j++
			}
			shards[i] = set[hashGids[j]]
		}
	}
	return shards
}

type ByUint32 []uint32

func (a ByUint32) Len() int           { return len(a) }
func (a ByUint32) Less(i, j int) bool { return a[i] < a[j] }
func (a ByUint32) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func Sharding(shards [NShards]int, newGidSet map[int]bool) [NShards]int {
	if len(newGidSet) == 0 {
		return [NShards]int{}
	}
	move := make([]int, 0)
	shardsPerGid := make(map[int][]int)
	for i, gid := range shards {
		if gid == 0 || !newGidSet[gid] {
			move = append(move, i)
		} else {
			shardsPerGid[gid] = append(shardsPerGid[gid], i)
		}
	}

	newGidArr := make([]int, 0)
	for k := range newGidSet {
		newGidArr = append(newGidArr, k)
	}

	sort.Ints(newGidArr)

	ave := len(shards) / len(newGidArr)
	left := len(shards) - ave*len(newGidArr)

	for _, gid := range newGidArr {
		s := shardsPerGid[gid]
		var threshold int
		if left > 0 {
			threshold = ave + 1
			left--
		} else {
			threshold = ave
		}
		if len(s) > threshold {
			move = append(move, s[threshold:]...)
			shardsPerGid[gid] = s[:threshold]
		}
	}

	left = len(shards) - ave*len(newGidArr)

	for _, gid := range newGidArr {
		s := shardsPerGid[gid]
		var threshold int
		if left > 0 {
			threshold = ave + 1
			left--
		} else {
			threshold = ave
		}
		if len(s) < threshold {
			for _, i := range move[:threshold-len(s)] {
				shards[i] = gid
			}
			move = move[threshold-len(s):]
		}
	}

	return shards
}
