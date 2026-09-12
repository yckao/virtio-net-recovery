// Package selection resolves explicit QEMU PIDs and running libvirt domains.
package selection

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type Target struct {
	PID       int    `json:"pid"`
	StartTime string `json:"start_time"`
	Domain    string `json:"domain,omitempty"`
	UUID      string `json:"uuid,omitempty"`
}

func (t Target) Key() string { return fmt.Sprintf("%d:%s", t.PID, t.StartTime) }

type Selector struct {
	PIDs       []int
	Pattern    string
	ProcRoot   string
	LibvirtDir string
	anchored   map[int]string
	re         *regexp.Regexp
}

func New(pids []int, pattern, procRoot, libvirtDir string) (*Selector, error) {
	if len(pids) == 0 && pattern == "" {
		return nil, errors.New("provide --pid or --domain-regex")
	}
	for _, pid := range pids {
		if pid <= 0 {
			return nil, errors.New("PIDs must be positive")
		}
	}
	var re *regexp.Regexp
	var err error
	if pattern != "" {
		re, err = regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("domain regex: %w", err)
		}
	}
	return &Selector{PIDs: slices.Clone(pids), Pattern: pattern, ProcRoot: procRoot,
		LibvirtDir: libvirtDir, re: re, anchored: map[int]string{}}, nil
}

func (s *Selector) process(pid int) (Target, error) {
	root := filepath.Join(s.ProcRoot, strconv.Itoa(pid))
	comm, err := os.ReadFile(filepath.Join(root, "comm"))
	if err != nil {
		return Target{}, err
	}
	if !strings.HasPrefix(strings.TrimSpace(string(comm)), "qemu-system") {
		return Target{}, errors.New("process is not QEMU")
	}
	stat, err := os.ReadFile(filepath.Join(root, "stat"))
	if err != nil {
		return Target{}, err
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return Target{}, errors.New("invalid process stat")
	}
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) < 20 {
		return Target{}, errors.New("incomplete process stat")
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil {
		return Target{}, err
	}
	args, err := os.ReadFile(filepath.Join(root, "cmdline"))
	if err != nil {
		return Target{}, err
	}
	argv := strings.Split(string(args), "\x00")
	t := Target{PID: pid, StartTime: fields[19]}
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == "-uuid" {
			t.UUID = strings.ToLower(argv[i+1])
		}
	}
	return t, nil
}

// Check closes the selection-to-attachment PID reuse window. Call again after
// acquiring the pidfd and before performing target operations.
func (s *Selector) Check(t Target) error {
	current, err := s.process(t.PID)
	if err != nil {
		return err
	}
	if current.StartTime != t.StartTime || current.UUID != t.UUID {
		return errors.New("QEMU identity changed after selection")
	}
	return nil
}

// Resolve returns individual target problems separately so one stopped VM does
// not stop monitoring the others. Explicit PID identities never follow reuse.
func (s *Selector) Resolve() ([]Target, []error, error) {
	found := map[int]Target{}
	var problems []error
	for _, pid := range s.PIDs {
		t, err := s.process(pid)
		if err != nil {
			problems = append(problems, fmt.Errorf("PID %d: %w", pid, err))
			continue
		}
		if start, ok := s.anchored[pid]; ok && start != t.StartTime {
			problems = append(problems, fmt.Errorf("PID %d was reused; select its new domain or restart with the new PID", pid))
			continue
		}
		s.anchored[pid] = t.StartTime
		found[pid] = t
	}
	if s.re != nil {
		entries, err := os.ReadDir(s.LibvirtDir)
		if err != nil {
			return nil, problems, fmt.Errorf("read libvirt runtime directory: %w", err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".xml") {
				continue
			}
			d, err := readDomain(filepath.Join(s.LibvirtDir, entry.Name()))
			if err != nil {
				problems = append(problems, fmt.Errorf("runtime file %s: %w", entry.Name(), err))
				continue
			}
			if !s.re.MatchString(d.Name) || d.State != "running" {
				continue
			}
			t, err := s.process(d.PID)
			if err != nil {
				problems = append(problems, fmt.Errorf("domain %s: %w", d.Name, err))
				continue
			}
			if d.UUID == "" || t.UUID != strings.ToLower(d.UUID) {
				problems = append(problems, fmt.Errorf("domain %s: runtime UUID does not match QEMU", d.Name))
				continue
			}
			t.Domain = d.Name
			found[t.PID] = t
		}
	}
	result := make([]Target, 0, len(found))
	for _, t := range found {
		result = append(result, t)
	}
	slices.SortFunc(result, func(a, b Target) int { return a.PID - b.PID })
	return result, problems, nil
}

type domainStatus struct {
	XMLName xml.Name `xml:"domstatus"`
	State   string   `xml:"state,attr"`
	PID     int      `xml:"pid,attr"`
	Name    string   `xml:"domain>name"`
	UUID    string   `xml:"domain>uuid"`
}

func readDomain(path string) (domainStatus, error) {
	f, err := os.Open(path)
	if err != nil {
		return domainStatus{}, err
	}
	defer f.Close()
	var d domainStatus
	err = xml.NewDecoder(io.LimitReader(f, 4<<20)).Decode(&d)
	if err == nil && (d.Name == "" || d.PID <= 0) {
		err = errors.New("missing domain name or PID")
	}
	return d, err
}

// PIDList accepts repeated --pid arguments and comma-separated values.
type PIDList []int

func (p *PIDList) String() string {
	var values []string
	for _, pid := range *p {
		values = append(values, strconv.Itoa(pid))
	}
	return strings.Join(values, ",")
}
func (p *PIDList) Set(value string) error {
	var values []int
	for _, item := range strings.Split(value, ",") {
		pid, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil || pid <= 0 {
			return fmt.Errorf("invalid QEMU PID %q", item)
		}
		values = append(values, pid)
	}
	for _, pid := range values {
		if !slices.Contains(*p, pid) {
			*p = append(*p, pid)
		}
	}
	return nil
}
