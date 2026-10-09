package cli

import (
	"context"

	"github.com/yckao/virtio-net-recovery/internal/agent/selection"
)

type commandSummary struct {
	Schema          int      `json:"schema_version"`
	Event           string   `json:"event"`
	Command         string   `json:"command"`
	Execution       string   `json:"execution"`
	Targets         int      `json:"targets"`
	Problems        []string `json:"problems,omitempty"`
	ProblemsOmitted int      `json:"problems_omitted,omitempty"`
}

func finishCommand(ctx context.Context, out *commandOutput, command string, inventory selection.Result, failed bool) error {
	incomplete := failed || !inventory.Complete || len(inventory.Targets) == 0 || len(inventory.Problems) != 0 || inventory.ProblemsOmitted != 0
	summary := commandSummary{Schema: 1, Event: "command_summary", Command: command, Execution: "complete", Targets: len(inventory.Targets), ProblemsOmitted: inventory.ProblemsOmitted}
	if incomplete {
		summary.Execution = "incomplete"
	}
	for _, problem := range inventory.Problems {
		summary.Problems = append(summary.Problems, message(problem))
	}
	if err := out.deliver(ctx, summary); err != nil {
		return err
	}
	if incomplete {
		return errIncomplete
	}
	return nil
}
