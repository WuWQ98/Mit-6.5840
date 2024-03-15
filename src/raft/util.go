package raft

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Debugging
const Debug = false

var filename string

var lock sync.Mutex

func init() {
	filename = fmt.Sprintf("raft_%s.log", time.Now().Format("2006-01-02_15:04:05"))
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

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func EntriesEqual(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	equal := true
	for i, entry := range a {
		if entry.Term != b[i].Term || entry.Index != b[i].Index {
			equal = false
			break
		}
	}
	return equal
}
