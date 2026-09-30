//go:build linux && amd64

package main

import (
	"errors"
	"flag"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseConfigModesAndSafeAliases(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		mode string
	}{
		{"default", nil, "observe"},
		{"observe", []string{"--mode", "observe"}, "observe"},
		{"recover", []string{"--mode", "recover"}, "recover"},
		{"kick", []string{"--mode", "kick"}, "kick"},
		{"once", []string{"--once"}, "kick"},
		{"rescue", []string{"--rescue"}, "kick"},
		{"both kick aliases", []string{"--once", "--rescue", "--mode", "kick"}, "kick"},
		{"false alias does not kick", []string{"--once=false", "--rescue=false"}, "observe"},
		{"trace", []string{"--mode", "trace", "--duration", "30"}, "trace"},
		{"trace alias", []string{"--trace-stages", "--duration", "30"}, "trace"},
		{"explicit matching trace alias", []string{"--mode", "trace", "--trace-stages", "--duration", "30"}, "trace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig(append([]string{"--pid", "1234"}, tc.args...))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Agent.Mode != tc.mode {
				t.Fatalf("mode = %q, want %q", cfg.Agent.Mode, tc.mode)
			}
			if cfg.Agent.Interval != 0.1 || cfg.Agent.InventoryInterval != 5 || cfg.Refresh != 5*time.Second {
				t.Fatalf("unexpected cadence defaults: %+v", cfg)
			}
		})
	}
}

func TestParseConfigRejectsUnsafeOrRetiredCombinations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		message string
	}{
		{"once with observe", []string{"--once", "--mode", "observe"}, "conflict"},
		{"rescue with recover", []string{"--rescue", "--mode", "recover"}, "conflict"},
		{"trace alias with observe", []string{"--trace-stages", "--mode", "observe", "--duration", "30"}, "conflict"},
		{"trace alias with kick", []string{"--trace-stages", "--mode", "kick", "--duration", "30"}, "conflict"},
		{"kick and trace aliases", []string{"--once", "--trace-stages", "--duration", "30"}, "cannot be combined"},
		{"trace without duration", []string{"--mode", "trace"}, "explicit --duration"},
		{"trace alias without duration", []string{"--trace-stages"}, "explicit --duration"},
		{"trace below duration bound", []string{"--mode", "trace", "--duration", "0.5"}, "duration"},
		{"trace above duration bound", []string{"--mode", "trace", "--duration", "301"}, "duration"},
		{"guarded migration", []string{"--mode", "guarded"}, "recover"},
		{"periodic migration", []string{"--mode", "periodic"}, "kick"},
		{"threshold even at former default", []string{"--threshold", "3"}, "--threshold has been removed"},
		{"cooldown", []string{"--cooldown", "30"}, "--cooldown has been removed"},
		{"budget", []string{"--max-recoveries", "3"}, "--max-recoveries has been removed"},
		{"periodic interval", []string{"--kick-interval", "1"}, "--kick-interval has been removed"},
		{"false retired batching", []string{"--batch-rings=false"}, "--batch-rings has been removed"},
		{"positional", []string{"extra"}, "unexpected positional"},
		{"zero refresh", []string{"--target-interval", "0s"}, "positive"},
		{"FD outside kick mode", []string{"--vhost-fd", "42"}, "kick"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig(append([]string{"--pid", "1234"}, tc.args...))
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.message)
			}
		})
	}
}

func TestParseConfigSelectionAndExplicitCadence(t *testing.T) {
	cfg, err := parseConfig([]string{"--pid", "1234,5678", "--pid", "1234", "--domain-regex", "^worker-", "--mode", "recover", "--interval", "0.025", "--inventory-interval", "1", "--target-interval", "2s"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.Selector.PIDs, []int{1234, 5678}) || cfg.Selector.Pattern != "^worker-" {
		t.Fatalf("unexpected selection: %+v", cfg.Selector)
	}
	if cfg.Agent.Interval != 0.025 || cfg.Agent.InventoryInterval != 1 || cfg.Refresh != 2*time.Second {
		t.Fatalf("explicit cadence was not retained: %+v", cfg)
	}
}

func TestParseConfigHelpDoesNotRequireSelection(t *testing.T) {
	if _, err := parseConfig([]string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help error = %v", err)
	}
}
