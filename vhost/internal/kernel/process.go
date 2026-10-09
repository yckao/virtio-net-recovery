package kernel

import (
	"errors"
	"strconv"
	"strings"
)

// StartTime reads field 22 without splitting a parenthesized command name.
func StartTime(data []byte) (uint64, error) {
	text := string(data)
	end := strings.LastIndex(text, ")")
	if end < 0 {
		return 0, errors.New("invalid process stat")
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 20 {
		return 0, errors.New("short process stat")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

var ErrGeneration = errors.New("process generation changed")

type InventoryProblem struct {
	Slot int
	Err  error
}
type Inventory struct {
	Vhosts    []int
	Events    map[uint32][]int
	Problems  []InventoryProblem
	Complete  bool
	Truncated bool
}
