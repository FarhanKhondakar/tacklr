package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/interrupt"
)

// durableJobPollInterval is how often a durable job's status is polled to
// reflect interrupted/terminal transitions onto the local job record.
const durableJobPollInterval = 250 * time.Millisecond

// This file wires async spawn_worker jobs (block=false) onto a durable
// Executor when AgentOptions.Durable is set. The in-process path in jobs.go
// stays the default when Durable is nil.

// DurableStepRunnerFactory builds a harness for one durable worker step. It
// runs in the durable backend's worker process, which may be a different
// process from the parent harness, so it cannot close over parent internals.
// name is the worker spec name; id is the job id (the spawn tool-call id).
//
// Implementations construct the AgentOptions for the worker (model, tools,
// store, and the inherited session world) from the durable spec plus their
// own host configuration.
type DurableStepRunnerFactory func(name string, id string) (AgentOptions, error)

// DurableStepRunner returns a durable.StepRunner that runs one durable
// spawn_worker step. Each step:
//   - builds a fresh worker harness via factory (resumes the checkpointed
//     worker session when Kind is StepResume)
//   - runs the worker and drains its events into outcome.Events
//   - persists the worker session checkpoint (via the harness's normal defer)
//   - returns Complete with the worker output, or Interrupted with the park
//     state (child interrupt ids + serialized primary interrupt)
//
// The harness itself owns checkpoint-on-exit; the step adds no durability.
func DurableStepRunner(factory DurableStepRunnerFactory) (durable.StepRunner, error) {
	if factory == nil {
		return nil, fmt.Errorf("durable step runner: factory is required")
	}
	return func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		spec := in.Spec
		opts, err := factory(spec.WorkerName, spec.ID)
		if err != nil {
			return durable.StepOutcome{}, fmt.Errorf("build worker options: %w", err)
		}
		workerSession := workerSessionID(spec.SessionID, spec.WorkerName, spec.ID)
		opts.SessionID = workerSession

		// On resume, rebuild the worker from its checkpointed session so its
		// pending interrupts are present for ReturnFromInterrupt. On start the
		// session is fresh. Both paths persist the checkpoint on exit via the
		// harness's normal defer.
		var worker *AgentHarness
		if in.Kind == durable.StepResume {
			worker, err = NewAgentFromSession(ctx, workerSession, opts)
			if err != nil {
				return durable.StepOutcome{}, fmt.Errorf("resume worker %q session %q: %w", spec.WorkerName, workerSession, err)
			}
		} else {
			worker, err = NewAgent(ctx, opts)
			if err != nil {
				return durable.StepOutcome{}, fmt.Errorf("build worker: %w", err)
			}
		}
		defer worker.Close()

		var events <-chan StreamEvent
		if in.Kind == durable.StepResume {
			events, err = worker.ReturnFromInterrupt(ctx, in.Resolutions)
		} else {
			events, err = worker.Run(ctx, spec.Task)
		}
		if err != nil {
			return durable.StepOutcome{}, fmt.Errorf("start worker %q: %w", spec.WorkerName, err)
		}

		drained, drainErr := drainWorkerEventsCollect(ctx, spec.WorkerName, events, nil, true)
		out := durable.StepOutcome{}
		if ctx.Err() != nil {
			out.Err = ctx.Err().Error()
			return out, nil
		}
		if drainErr != nil {
			out.Err = fmt.Errorf("worker %q: %w", spec.WorkerName, drainErr).Error()
			return out, nil
		}
		out.Events = drained.events
		if drained.completed {
			result := finalWorkerOutput(worker.Messages(), drained.lastAssistant)
			if result == "" {
				out.Err = fmt.Sprintf("worker %q: no output", spec.WorkerName)
				return out, nil
			}
			out.Complete = true
			out.Output = result
			return out, nil
		}

		intrState, ierr := durableInterruptState(worker, drained.interruptIDs)
		if ierr != nil {
			out.Err = ierr.Error()
			return out, nil
		}
		out.Interrupted = intrState
		return out, nil
	}, nil
}

// durableInterruptState builds the durable park state for an interrupted
// worker step: the child interrupt ids plus the serialized primary interrupt.
func durableInterruptState(worker *AgentHarness, interruptIDs []string) (*durable.InterruptState, error) {
	ids, primary := collectChildInterrupts(worker, interruptIDs)
	if primary == nil {
		return nil, fmt.Errorf("worker incomplete: %w", ErrFailed)
	}
	env, err := interrupt.EncodeEnvelope(primary)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal interrupt envelope: %w", err)
	}
	return &durable.InterruptState{
		ChildInterruptIDs: ids,
		Payload:           payload,
		WorkerSessionID:   worker.sessionId,
	}, nil
}

// --- executor-backed job path ---

// scheduleDurableWorker starts an async worker job on the durable Executor.
func (a *AgentHarness) scheduleDurableWorker(workerName, task, jobID string, runtime HarnessRuntime) (string, error) {
	if _, ok := a.subagents[workerName]; !ok {
		return "", fmt.Errorf("worker %q: %w", workerName, ErrNotFound)
	}
	if strings.TrimSpace(task) == "" {
		return "", fmt.Errorf("worker %q: empty task: %w", workerName, ErrInvalid)
	}
	if existing := a.getJob(jobID); existing != nil {
		return "", fmt.Errorf("job %q: already exists: %w", jobID, ErrInvalid)
	}

	handle, err := a.durable.Start(a.jobsCtxOrBackground(), durable.RunSpec{
		ID:         jobID,
		Kind:       durable.RunKindWorkerJob,
		SessionID:  a.sessionId,
		WorkerName: workerName,
		Task:       task,
	})
	if err != nil {
		return "", fmt.Errorf("worker %q: start durable run: %w", workerName, err)
	}

	j := &workerRun{
		id:         jobID,
		workerName: workerName,
		task:       task,
		mode:       workerDeliveryAsync,
		status:     jobStatusRunning,
		durable:    handle,
		done:       make(chan struct{}),
	}
	a.registerJob(j)
	go a.watchDurableJob(j)

	runtime.EmitUpdate(fmt.Sprintf("Job %s scheduled (worker=%s, durable)", jobID, workerName))
	return fmt.Sprintf("Job %s scheduled (worker=%s). Use list_jobs to poll, get_job to collect its result (block=true to wait), or cancel_job to stop it.", jobID, workerName), nil
}

// watchDurableJob mirrors the durable run's state onto the job so
// list_jobs/get_job report interrupted/terminal state without each caller
// blocking on the backend. It polls Status (cheap) until the run parks or
// terminates, then resolves the result.
func (a *AgentHarness) watchDurableJob(j *workerRun) {
	ticker := time.NewTicker(durableJobPollInterval)
	defer ticker.Stop()
	for {
		st, err := j.durable.Status(context.Background())
		if err == nil {
			switch st {
			case durable.StatusInterrupted:
				j.mu.Lock()
				if j.status == jobStatusRunning {
					j.status = jobStatusInterrupted
				}
				j.mu.Unlock()
			case durable.StatusCompleted, durable.StatusFailed:
				a.finishDurableJob(j)
				return
			}
		}
		select {
		case <-a.jobsCtxOrBackground().Done():
			return
		case <-j.done:
			return
		case <-ticker.C:
		}
	}
}

// finishDurableJob resolves the terminal result of a finished durable run.
func (a *AgentHarness) finishDurableJob(j *workerRun) {
	res, err := j.durable.Result(context.Background())
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.status != jobStatusRunning && j.status != jobStatusInterrupted {
		return
	}
	if err != nil {
		j.status = jobStatusFailed
		j.err = err
	} else if res.Status == durable.StatusCompleted {
		j.status = jobStatusCompleted
		j.result = res.Output
	} else {
		j.status = jobStatusFailed
		j.err = res.Err
	}
	select {
	case <-j.done:
	default:
		close(j.done)
	}
}

func (a *AgentHarness) jobsCtxOrBackground() context.Context {
	if a.jobsCtx != nil {
		return a.jobsCtx
	}
	return context.Background()
}

// readDurableJob implements get_job for a durable run. block waits for the
// run to reach a terminal state; an interrupted run is resumed via Signal.
func (a *AgentHarness) readDurableJob(ctx context.Context, j *workerRun, block bool, runtime HarnessRuntime) (string, error) {
	st, err := j.durable.Status(ctx)
	if err != nil {
		return "", err
	}
	if !block {
		return a.formatJob(j.id)
	}

	switch st {
	case durable.StatusInterrupted:
		return a.resumeDurableJob(ctx, j, runtime)
	case durable.StatusRunning:
		runtime.EmitUpdate(fmt.Sprintf("Awaiting job %s", j.id))
	}
	res, err := j.durable.Result(ctx)
	if err != nil {
		a.removeJob(j.id)
		return "", err
	}
	if res.Status == durable.StatusInterrupted {
		return a.resumeDurableJob(ctx, j, runtime)
	}
	a.removeJob(j.id)
	if res.Status != durable.StatusCompleted {
		if res.Err == nil {
			res.Err = fmt.Errorf("worker %q failed: %w", j.workerName, ErrFailed)
		}
		return "", res.Err
	}
	return res.Output, nil
}

// resumeDurableJob resolves the job's parked interrupt on the parent session
// (parking the get_job tool call) or forwards the resolution on re-entry.
func (a *AgentHarness) resumeDurableJob(ctx context.Context, j *workerRun, runtime HarnessRuntime) (string, error) {
	toolCallID := runtime.CurrentToolCallID()

	// Resolve the interrupt for this get_job call. On first entry this parks;
	// on re-entry (after ReturnFromInterrupt) it returns the resolved object.
	// The run's interrupt state carries the child's pending interrupt ids; the
	// resume payload must be routed to those, not the job id.
	state, err := j.durable.InterruptState(ctx)
	if err != nil {
		return "", fmt.Errorf("job %q: interrupt state: %w", j.id, err)
	}
	childIDs := j.childIntrIDs
	if state != nil && len(state.ChildInterruptIDs) > 0 {
		childIDs = state.ChildInterruptIDs
	}

	resolved, err := a.session.AdoptInterrupt(toolCallID, durableInterruptPlaceholder{JobID: j.id, ChildInterruptIDs: childIDs})
	if err != nil {
		return "", err
	}
	if resolved == nil {
		return "", fmt.Errorf("job %q: adopt returned nil: %w", j.id, ErrFailed)
	}

	// On re-entry the resolution payload was stashed by ReturnFromInterrupt.
	payload := a.interruptPayloads[toolCallID]
	if err := j.durable.Signal(ctx, durable.Signal{
		Name:        durable.SignalResume,
		Resolutions: payloadToResolutions(childIDs, payload),
	}); err != nil {
		return "", fmt.Errorf("job %q: resume: %w", j.id, err)
	}
	runtime.EmitUpdate(fmt.Sprintf("Job %s resumed", j.id))

	res, err := j.durable.Result(ctx)
	if err != nil {
		a.removeJob(j.id)
		return "", err
	}
	a.removeJob(j.id)
	if res.Status != durable.StatusCompleted {
		if res.Err == nil {
			res.Err = fmt.Errorf("worker %q failed: %w", j.workerName, ErrFailed)
		}
		return "", res.Err
	}
	return res.Output, nil
}

// payloadToResolutions forwards the parent's resolution payload to every child
// interrupt id recorded at park time, so the worker harness's
// ReturnFromInterrupt applies it to the correct pending interrupt.
func payloadToResolutions(childIDs []string, payload []byte) map[string][]byte {
	if len(payload) == 0 || len(childIDs) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(childIDs))
	for _, id := range childIDs {
		out[id] = payload
	}
	return out
}

// durableInterruptPlaceholder is the interrupt parked on the parent's get_job
// tool call while a durable job awaits user input. It carries the job id plus
// the child's pending interrupt ids so the resume Signal can route the
// consumer payload to the correct interrupt(s) on the worker harness.
type durableInterruptPlaceholder struct {
	JobID             string   `json:"jobId"`
	ChildInterruptIDs []string `json:"childInterruptIds,omitempty"`
}

func (d durableInterruptPlaceholder) TypeName() string { return "durable_job_wait" }
func (d durableInterruptPlaceholder) Serialize() ([]byte, error) {
	return json.Marshal(d)
}
func (d durableInterruptPlaceholder) Return([]byte) error { return nil }
func (d durableInterruptPlaceholder) Error() string {
	b, _ := json.Marshal(d)
	return string(b)
}

func init() {
	interrupt.Register(func() interrupt.Interrupt { return &durableInterruptPlaceholder{} })
}
