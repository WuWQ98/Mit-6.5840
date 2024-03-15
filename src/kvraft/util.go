package kvraft

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
	filename = fmt.Sprintf("kvraft_%s.log", time.Now().Format("2006-01-02_15:04:05"))
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
