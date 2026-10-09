package discovery

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func (s *Selector) domains(ctx context.Context, r *resolution) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := os.Open(s.options.RuntimeDir)
	if err != nil {
		return fmt.Errorf("runtime inventory: %w", err)
	}
	defer dir.Close()
	checked := 0
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if checked == MaxRuntimeEntries {
				r.excluded(0, "", errors.New("runtime entry scan limit reached"), false)
				return nil
			}
			checked++
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".xml") {
				continue
			}
			s.domain(entry.Name(), r)
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("runtime inventory: %w", readErr)
		}
	}
}

func (s *Selector) domain(name string, r *resolution) {
	d, err := readDomain(filepath.Join(s.options.RuntimeDir, name))
	if err != nil {
		r.excluded(0, "", fmt.Errorf("runtime file %s: %w", name, err), false)
		return
	}
	if d.State != "running" || !s.pattern.MatchString(d.Name) {
		return
	}
	t, err := s.process(d.PID)
	definitive := definitiveExclusion(err)
	if err == nil && (d.UUID == "" || t.UUID != strings.ToLower(d.UUID)) {
		err = errors.New("runtime UUID differs from process")
		definitive = true
	}
	if err != nil {
		r.excluded(d.PID, d.Name, err, definitive)
		return
	}
	t.Domain = d.Name
	r.add(t)
}

type domainStatus struct {
	XMLName xml.Name `xml:"domstatus"`
	State   string   `xml:"state,attr"`
	PID     int      `xml:"pid,attr"`
	Name    string   `xml:"domain>name"`
	UUID    string   `xml:"domain>uuid"`
}

func readDomain(path string) (domainStatus, error) {
	data, err := readSmall(path, 4<<20)
	if err != nil {
		return domainStatus{}, err
	}
	var d domainStatus
	if err = xml.Unmarshal(data, &d); err != nil {
		return d, err
	}
	if d.PID <= 0 || d.Name == "" {
		return d, errors.New("missing domain name or PID")
	}
	if len(d.Name) > MaxDomainNameBytes || len(d.UUID) > MaxUUIDBytes || len(d.State) > 32 {
		return domainStatus{}, errors.New("runtime identity exceeds size limit")
	}
	return d, nil
}
