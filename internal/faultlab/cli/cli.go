// Package cli is the composition boundary for the experiment executable.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
)

const (
	ExitOK = iota
	ExitOperation
	ExitUsage
	ExitDelivery
)

// Run dispatches one command. Every handler finishes owned kernel cleanup before
// presenting its result, so delivery failure cannot suppress restoration.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: vhost-faultlab prepare|run|show|clear [options]")
		return ExitUsage
	}
	switch args[0] {
	case "help", "--help", "-h":
		if _, err := fmt.Fprintln(stdout, "vhost-faultlab prepare|run|show|clear\nBounded Linux amd64 experiment. run always unloads before guarded restoration. clear unloads only."); err != nil {
			return ExitDelivery
		}
		return ExitOK
	case "prepare":
		return runPrepare(ctx, args[1:], stdout, stderr)
	case "run":
		return runFault(ctx, args[1:], stdout, stderr)
	case "show":
		return runShow(ctx, args[1:], stdout, stderr)
	case "clear":
		return runClear(ctx, args[1:], stdout, stderr)
	default:
		fmt.Fprintln(stderr, "unknown command")
		return ExitUsage
	}
}

func flags(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		err := errors.New("unexpected positional arguments")
		fmt.Fprintln(fs.Output(), err)
		return err
	}
	return nil
}

func parseExit(err error) int {
	if errors.Is(err, flag.ErrHelp) {
		return ExitOK
	}
	return ExitUsage
}

func deliver(stdout, stderr io.Writer, value any, operationErr error) int {
	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		fmt.Fprintln(stderr, "result delivery incomplete; operation may have taken effect:", err)
		return ExitDelivery
	}
	if operationErr != nil {
		return ExitOperation
	}
	return ExitOK
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
