package xtrss

import (
	"sync"
	"fmt"
)

type Params struct {
	Host     string
	NReqs    int
	Interval int
	Extent   int
	Bin      bool
	Verbose  bool
}

var units map[int]string = map[int]string{0: "B", 1: "kB", 2: "MB", 3: "TB", 4: "PB"}

func GetHumanSize(size uint64) string {
	i := 0
	for size > 1024 {
		size = size >> 10
		i += 1
	}

	return fmt.Sprintf("%d %s", size, units[i])
}

var dapBin map[bool]string = map[bool]string{true: "dods", false: "ascii"}

type ConcCounter struct {
	*sync.Mutex
	Total uint64
}

func (counter *ConcCounter) Add(size int) {
	counter.Lock()
	counter.Total += uint64(size)
	counter.Unlock()
}

func NewCounter() ConcCounter {
	return ConcCounter{&sync.Mutex{}, 0}
}
