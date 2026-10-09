package experiment_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-faultlab/internal/experiment"
)

type fixture struct {
	calls       []string
	present     bool
	load        experiment.LoadResult
	stats       experiment.Statistics
	statsErr    error
	unloadErr   error
	restore     experiment.RestoreResult
	waitErr     error
	afterArm    func()
	cleanupLive bool
}

func (f *fixture) Present(context.Context) (bool, error) {
	f.calls = append(f.calls, "present")
	return f.present, nil
}
func (f *fixture) Arm(context.Context, experiment.Plan) experiment.LoadResult {
	f.calls = append(f.calls, "arm")
	if f.afterArm != nil {
		f.afterArm()
	}
	return f.load
}
func (f *fixture) Statistics(ctx context.Context) (experiment.Statistics, error) {
	f.calls = append(f.calls, "stats")
	f.cleanupLive = ctx.Err() == nil
	if _, ok := ctx.Deadline(); !ok {
		return experiment.Statistics{}, errors.New("cleanup has no deadline")
	}
	return f.stats, f.statsErr
}
func (f *fixture) Disarm(context.Context) error {
	f.calls = append(f.calls, "disarm")
	return f.unloadErr
}
func (f *fixture) Restore(context.Context) experiment.RestoreResult {
	f.calls = append(f.calls, "restore")
	return f.restore
}
func (f *fixture) Wait(ctx context.Context, duration time.Duration) error {
	f.calls = append(f.calls, "wait")
	if duration != 110*time.Millisecond {
		return errors.New("incorrect kernel-window wait")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.waitErr
}

func setup(t *testing.T) (*fixture, *experiment.Controller, experiment.Plan) {
	t.Helper()
	f := &fixture{load: experiment.LoadResult{Loaded: true}, stats: experiment.Statistics{Dropped: 1, Matched: 2},
		restore: experiment.RestoreResult{Outcome: experiment.RestoreAccepted}}
	c, err := experiment.New(f, f, f)
	if err != nil {
		t.Fatal(err)
	}
	return f, c, experiment.Plan{Delay: 10 * time.Millisecond, Window: 100 * time.Millisecond, MaxDrops: 1, CleanupTimeout: time.Second}
}

func TestRunUnloadsBeforeFreshRestoration(t *testing.T) {
	f, controller, plan := setup(t)
	r := controller.Run(context.Background(), plan)
	want := []string{"present", "arm", "wait", "stats", "disarm", "restore"}
	if !reflect.DeepEqual(f.calls, want) || r.Err() != nil || !r.Loaded || !r.Disarmed || !r.StatisticsKnown || r.Restore.Outcome != experiment.RestoreAccepted {
		t.Fatalf("wrong lifecycle: calls=%v report=%+v", f.calls, r)
	}
}

func TestCancelledAcceptedLoadStillOwnsCleanup(t *testing.T) {
	f, controller, plan := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.afterArm = cancel
	r := controller.Run(ctx, plan)
	if !errors.Is(r.Err(), context.Canceled) || !r.Disarmed || r.Restore.Outcome != experiment.RestoreAccepted || !f.cleanupLive {
		t.Fatalf("request cancellation leaked accepted load: %+v", r)
	}
}

func TestAcceptedLoadWithErrorStillUnloads(t *testing.T) {
	f, controller, plan := setup(t)
	f.load.Err = errors.New("cancelled after kernel accepted load")
	r := controller.Run(context.Background(), plan)
	if !r.Loaded || !r.Disarmed || r.Err() == nil || !reflect.DeepEqual(f.calls, []string{"present", "arm", "stats", "disarm", "restore"}) {
		t.Fatalf("accepted load error lost cleanup ownership: %+v %v", r, f.calls)
	}
}

func TestFailedUnloadNeverRestores(t *testing.T) {
	f, controller, plan := setup(t)
	f.unloadErr = errors.New("module remains busy")
	r := controller.Run(context.Background(), plan)
	if r.Disarmed || r.Restore.Outcome != experiment.RestoreNotAttempted || !errors.Is(r.Err(), f.unloadErr) {
		t.Fatal("failed unload was reported as safe restoration")
	}
	for _, call := range f.calls {
		if call == "restore" {
			t.Fatal("restoration ran while injector might remain active")
		}
	}
}

func TestStatisticsFailureDoesNotSkipUnload(t *testing.T) {
	f, controller, plan := setup(t)
	f.statsErr = errors.New("stats unavailable")
	r := controller.Run(context.Background(), plan)
	if !r.Disarmed || r.Restore.Outcome != experiment.RestoreAccepted || r.StatisticsKnown || !errors.Is(r.Err(), f.statsErr) {
		t.Fatalf("stats failure interfered with cleanup: %+v", r)
	}
}

func TestNoDropsIsNotSuccessfulInjection(t *testing.T) {
	f, controller, plan := setup(t)
	f.stats.Dropped = 0
	r := controller.Run(context.Background(), plan)
	if r.Err() == nil || !r.Disarmed || r.Restore.Outcome != experiment.RestoreAccepted {
		t.Fatal("zero drops claimed success or suppressed cleanup")
	}
}

func TestPreexistingModuleIsNeverAdoptedOrUnloaded(t *testing.T) {
	f, controller, plan := setup(t)
	f.present = true
	r := controller.Run(context.Background(), plan)
	if r.Loaded || r.Disarmed || r.Err() == nil || !reflect.DeepEqual(f.calls, []string{"present"}) {
		t.Fatal("controller adopted another run's module")
	}
}

func TestUnacceptedLoadDoesNotAttemptCleanupOrRestoration(t *testing.T) {
	f, controller, plan := setup(t)
	f.load = experiment.LoadResult{Err: errors.New("unsupported probe")}
	r := controller.Run(context.Background(), plan)
	if r.Loaded || r.Err() == nil || !reflect.DeepEqual(f.calls, []string{"present", "arm"}) {
		t.Fatal("failed load assumed cleanup ownership")
	}
}

func TestInvalidBoundsAndPriorCancellationHaveNoEffects(t *testing.T) {
	for _, mutate := range []func(*experiment.Plan){
		func(p *experiment.Plan) { p.Delay = -1 },
		func(p *experiment.Plan) { p.Delay = time.Minute + time.Millisecond },
		func(p *experiment.Plan) { p.Window = time.Nanosecond },
		func(p *experiment.Plan) { p.MaxDrops = 0 },
		func(p *experiment.Plan) { p.MaxDrops = 1_000_001 },
		func(p *experiment.Plan) { p.CleanupTimeout = 0 },
	} {
		f, controller, plan := setup(t)
		mutate(&plan)
		if r := controller.Run(context.Background(), plan); r.Err() == nil || len(f.calls) != 0 {
			t.Fatal("invalid bounds caused effects")
		}
	}
	f, controller, plan := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r := controller.Run(ctx, plan); !errors.Is(r.Err(), context.Canceled) || len(f.calls) != 0 {
		t.Fatal("cancelled start caused effects")
	}
}
