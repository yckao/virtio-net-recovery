package cli

import (
	"context"
	"io"
)

type processRecord struct {
	Schema   int          `json:"schema_version"`
	Command  string       `json:"command"`
	Target   targetRecord `json:"target"`
	Complete bool         `json:"complete"`
}

func runList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var o selectionOptions
	f := flags("list", stderr)
	o.flags(f)
	if err := parseFlags(f, args); err != nil {
		return err
	}
	selector, err := o.selector()
	if err != nil {
		return err
	}
	inventory, err := selector.Resolve(ctx)
	if err != nil {
		return err
	}
	out := newOutput(stdout)
	defer out.Close()
	for _, target := range inventory.Targets {
		if err := out.deliver(ctx, processRecord{Schema: 1, Command: "list", Target: targetDTO(target), Complete: true}); err != nil {
			return err
		}
	}
	return finishCommand(ctx, out, "list", inventory, false)
}
