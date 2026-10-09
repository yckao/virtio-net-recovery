package discovery

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func readSmall(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(b)) > limit {
		return nil, errors.New("input exceeds size limit")
	}
	return b, err
}
func (s *Selector) process(pid int) (Target, error) {
	root := filepath.Join(s.options.ProcRoot, strconv.Itoa(pid))
	// Bracket multi-file observation with stat reads; still only a discovery hint.
	first, err := readSmall(filepath.Join(root, "stat"), 65536)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if _, statErr := os.Stat(root); errors.Is(statErr, os.ErrNotExist) {
				return Target{}, errProcessAbsent
			}
		}
		return Target{}, err
	}
	start, err := startTime(first)
	if err != nil {
		return Target{}, err
	}
	comm, err := readSmall(filepath.Join(root, "comm"), 256)
	if err != nil {
		return Target{}, err
	}
	isQEMU := strings.HasPrefix(strings.TrimSpace(string(comm)), "qemu-system")
	args, err := readSmall(filepath.Join(root, "cmdline"), 1<<20)
	if err != nil {
		return Target{}, err
	}
	last, err := readSmall(filepath.Join(root, "stat"), 65536)
	if err != nil {
		return Target{}, err
	}
	current, err := startTime(last)
	if err != nil {
		return Target{}, err
	}
	if current != start {
		return Target{}, errors.New("process changed during discovery")
	}
	if !isQEMU {
		return Target{}, errNotQEMU
	}
	t := Target{PID: pid, StartTime: start}
	// Scan one argument at a time so a NUL-heavy cmdline cannot allocate a
	// slice entry for every byte of the bounded input.
	remaining := string(args)
	for remaining != "" {
		argument, rest, terminated := strings.Cut(remaining, "\x00")
		remaining = rest
		if argument == "-uuid" && terminated {
			uuid, _, _ := strings.Cut(rest, "\x00")
			if len(uuid) > MaxUUIDBytes {
				return Target{}, errors.New("process UUID exceeds size limit")
			}
			t.UUID = strings.ToLower(uuid)
		}
	}
	return t, nil
}

var (
	errProcessAbsent = errors.New("process is absent")
	errNotQEMU       = errors.New("process is not QEMU")
)

func definitiveExclusion(err error) bool {
	return errors.Is(err, errProcessAbsent) || errors.Is(err, errNotQEMU)
}
func startTime(data []byte) (uint64, error) {
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, errors.New("invalid process stat")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return 0, errors.New("incomplete process stat")
	}
	value, err := strconv.ParseUint(fields[19], 10, 64)
	if err == nil && value == 0 {
		return 0, errors.New("invalid process generation")
	}
	return value, err
}
