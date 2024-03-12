package kvraft

import (
	"fmt"
	"os"
	"sync"
)

// Debugging
const Debug = false

const filename = "kvraft.log"

var lock sync.Mutex

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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
