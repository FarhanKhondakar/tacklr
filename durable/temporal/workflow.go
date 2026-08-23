package temporal

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr/durable"
)

// TacklrRunWorkflow is the deterministic durable driver for one harness run
// (a background worker job or a session turn). The non-deterministic harness
// work runs inside RunStep activities; the workflow only orchestrates: start,
// then on each parked interrupt wait for the resume signal and run the resume
// step, until the run completes.
//
// The workflow keeps no conversation state; the harness checkpoint (Postgres
// store) is the source of truth, and each RunStep rebuilds the harness from
// it, so activity retries after a crash are idempotent.
func TacklrRunWorkflow(ctx workflow.Context, spec durable.RunSpec) (durable.RunResult, error) {
	logger := workflow.GetLogger(ctx)

	ao := workflow.ActivityOptions{
		// A step is one full harness Run/ReturnFromInterrupt; it can park for a
		// long time only between steps, never inside one, so a generous bound is
		// safe. Heartbeats carry liveness; HeartbeatTimeout bounds a hung step.
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    2 * time.Minute,
		// Blind retries would re-run non-deterministic work; the checkpoint
		// makes a retry safe, so allow a few, but never retry terminal failures.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
			NonRetryableErrorTypes: []string{
				"ErrInvalid", "ErrNotFound", "ErrFailed",
			},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	step := durable.StepInput{Spec: spec, Kind: durable.StepStart}
	if len(spec.Resolutions) > 0 {
		step.Kind = durable.StepResume
		step.Resolutions = spec.Resolutions
	}

	resumeCh := workflow.GetSignalChannel(ctx, durable.SignalResume)

	for {
		var outcome durable.StepOutcome
		err := workflow.ExecuteActivity(ctx, ActivityRunStep, step).Get(ctx, &outcome)
		if err != nil {
			// Cancellation and terminal activity failures both surface here.
			return durable.RunResult{Status: durable.StatusFailed, Err: err}, err
		}
		if outcome.Err != "" {
			logger.Info("run step failed", "run_id", spec.ID, "error", outcome.Err)
			return durable.RunResult{Status: durable.StatusFailed, Err: errors.New(outcome.Err)}, nil
		}
		if outcome.Complete {
			return durable.RunResult{Status: durable.StatusCompleted, Output: outcome.Output}, nil
		}
		if outcome.Interrupted == nil {
			logger.Error("run step returned neither Complete nor Interrupted", "run_id", spec.ID)
			return durable.RunResult{Status: durable.StatusFailed, Err: errors.New("step returned no outcome")}, nil
		}

		// Parked: wait for the resume signal, or workflow cancellation. The
		// selector must also select on ctx.Done so CancelWorkflow wakes it.
		logger.Info("run interrupted, awaiting resume", "run_id", spec.ID,
			"child_interrupts", len(outcome.Interrupted.ChildInterruptIDs))
		var resolutions map[string][]byte
		selector := workflow.NewSelector(ctx)
		resumed := false
		selector.AddReceive(resumeCh, func(c workflow.ReceiveChannel, more bool) {
			var sig durable.Signal
			c.Receive(ctx, &sig)
			resolutions = sig.Resolutions
			resumed = true
		})
		selector.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, more bool) {})
		selector.Select(ctx)
		if err := ctx.Err(); err != nil {
			return durable.RunResult{Status: durable.StatusFailed, Err: err}, err
		}
		if !resumed {
			continue
		}
		step = durable.StepInput{Spec: spec, Kind: durable.StepResume, Resolutions: resolutions}
	}
}

// ActivityRunStep is the registered name of the RunStep activity. The concrete
// implementation is the injected durable.StepRunner (see worker.go), so the
// Temporal package never imports the harness.
const ActivityRunStep = "tacklr.RunStep"

// stepHeartbeatInterval is how often a running step heartbeats. It must be
// well under the activity HeartbeatTimeout so a long harness turn keeps the
// activity alive while work is genuinely progressing.
const stepHeartbeatInterval = 5 * time.Second

// RunStepActivity adapts a durable.StepRunner to a Temporal activity. A harness
// step can run for minutes (a full worker turn), so it runs the runner in the
// background and heartbeats on a ticker until it returns; on cancellation the
// heartbeat loop stops and the runner's ctx is already cancelled by Temporal.
func RunStepActivity(runner durable.StepRunner) func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
	return func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		if runner == nil {
			return durable.StepOutcome{}, temporal.NewApplicationError("no StepRunner registered", "ErrInvalid")
		}

		type stepResult struct {
			out durable.StepOutcome
			err error
		}
		resultCh := make(chan stepResult, 1)
		go func() {
			out, err := runner(ctx, in)
			resultCh <- stepResult{out: out, err: err}
		}()

		ticker := time.NewTicker(stepHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return durable.StepOutcome{}, ctx.Err()
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, "step")
			case res := <-resultCh:
				return res.out, res.err
			}
		}
	}
}
