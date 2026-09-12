//go:build linux && amd64

package fault

import (
	"testing"
	"time"

	"vhost-watch/internal/selection"
)

func TestInjectionBounds(t *testing.T) {
	selector, err := selection.New([]int{123}, "", "/proc", "/run/libvirt/qemu")
	if err != nil {
		t.Fatal(err)
	}
	base := Config{Selector: selector, VhostFD: 42, Delay: time.Second, Window: time.Second, Drops: 1, RecoverAfter: 30 * time.Second}
	for _, tc := range []struct {
		name   string
		change func(*Config)
		valid  bool
	}{
		{"default", func(c *Config) {}, true},
		{"manual recovery", func(c *Config) { c.RecoverAfter = 0 }, true},
		{"missing FD", func(c *Config) { c.VhostFD = -1 }, false},
		{"no selector", func(c *Config) { c.Selector = nil }, false},
		{"unbounded window", func(c *Config) { c.Window = 61 * time.Second }, false},
		{"empty window", func(c *Config) { c.Window = 0 }, false},
		{"sub millisecond window", func(c *Config) { c.Window = time.Microsecond }, false},
		{"negative delay", func(c *Config) { c.Delay = -time.Millisecond }, false},
		{"excessive delay", func(c *Config) { c.Delay = 61 * time.Second }, false},
		{"fractional delay", func(c *Config) { c.Delay = time.Microsecond }, false},
		{"zero drops", func(c *Config) { c.Drops = 0 }, false},
		{"excessive drops", func(c *Config) { c.Drops = 1000001 }, false},
		{"negative deadline", func(c *Config) { c.RecoverAfter = -time.Second }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.change(&c)
			if err := c.validate(); (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
}
