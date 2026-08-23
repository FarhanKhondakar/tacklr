package temporal

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Executor is a durable.Executor backed by a Temporal service. It schedules
// TacklrRunWorkflow executions and exposes signals/cancel/result over the
// Temporal client. The workflow code runs on a WorkerHost (see worker.go);
// this type is only the client-facing handle.
type Executor struct {
	client    client.Client
	taskQueue string
	// bridge is the in-process live-event bridge, wired when the executor is
	// created from a WorkerHost in the same process. Nil means replay-only.
	bridge *eventBridge
}

// NewExecutor dials the Temporal service and returns an Executor. The caller
// owns the client lifecycle via Close.
func NewExecutor(ctx context.Context, cfg Config) (*Executor, error) {
	opts, err := cfg.ClientOptions()
	if err != nil {
		return nil, err
	}
	c, err := client.DialContext(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("temporal: dial: %w", err)
	}
	return &Executor{client: c, taskQueue: cfg.TaskQueueName()}, nil
}

// NewExecutorWithClient wraps an existing client (shared with a WorkerHost).
func NewExecutorWithClient(c client.Client, taskQueue string) *Executor {
	return &Executor{client: c, taskQueue: taskQueue}
}

// Close releases the underlying client.
func (e *Executor) Close() { e.client.Close() }

// Start schedules a TacklrRunWorkflow for the spec and returns its handle.
// Re-Starting an existing ID attaches to the running execution.
func (e *Executor) Start(ctx context.Context, spec durable.RunSpec) (durable.RunHandle, error) {
	if spec.ID == "" {
		return nil, fmt.Errorf("temporal: run id is required")
	}
	opts := client.StartWorkflowOptions{
		ID:        spec.ID,
		TaskQueue: e.taskQueue,
		// A durable run id is a stable business key; reject reuse of a closed
		// id and error on an already-open one so list/get stay unambiguous.
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		SearchAttributes: map[string]any{
			"TacklrSessionID": spec.SessionID,
			"TacklrKind":      spec.Kind.String(),
			"TacklrWorker":    spec.WorkerName,
		},
	}
	_, err := e.client.ExecuteWorkflow(ctx, opts, TacklrRunWorkflow, spec)
	if err != nil {
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			// Idempotent attach to the existing run.
			return &handle{client: e.client, id: spec.ID, bridge: e.bridge}, nil
		}
		return nil, fmt.Errorf("temporal: start workflow %q: %w", spec.ID, err)
	}
	return &handle{client: e.client, id: spec.ID, bridge: e.bridge}, nil
}

// handle is a durable.RunHandle over the Temporal client.
type handle struct {
	client client.Client
	id     string
	bridge *eventBridge
}

func (h *handle) ID() string { return h.id }

func (h *handle) Status(ctx context.Context) (durable.Status, error) {
	// Awaiting-resume is workflow-visible only through the run's open state;
	// the interrupted state is carried in the last activity result. The cheap
	// durable answer is: running until terminal. Callers that need the
	// interrupted detail read it from the StepOutcome via the event stream.
	resp, err := h.client.DescribeWorkflowExecution(ctx, h.id, "")
	if err != nil {
		return durable.StatusFailed, fmt.Errorf("temporal: describe %q: %w", h.id, err)
	}
	switch resp.GetWorkflowExecutionInfo().GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return durable.StatusRunning, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return durable.StatusCompleted, nil
	default: // Failed, Canceled, Terminated, TimedOut, ContinuedAsNew
		return durable.StatusFailed, nil
	}
}

// InterruptState returns the park state from the most recent RunStep activity
// result in workflow history, or nil when the run is not interrupted.
func (h *handle) InterruptState(ctx context.Context) (*durable.InterruptState, error) {
	iter := h.client.GetWorkflowHistory(ctx, h.id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var last *durable.InterruptState
	seen := false
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("temporal: history %q: %w", h.id, err)
		}
		if event.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			continue
		}
		attrs := event.GetActivityTaskCompletedEventAttributes()
		if attrs == nil {
			continue
		}
		var outcome durable.StepOutcome
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &outcome); err != nil {
			continue
		}
		seen = true
		last = outcome.Interrupted
	}
	if !seen || last == nil {
		return nil, nil
	}
	return last, nil
}

// Events returns the live event stream for the run. When the executor was
// created from an in-process WorkerHost, events stream live through the
// bridge with a history-replay prefix (no loss or duplicates at the join).
// Standalone executors (worker deployed separately) fall back to history
// replay, so the client sees events only after each step completes.
func (h *handle) Events(ctx context.Context) (<-chan durable.BridgedEvent, error) {
	ch := make(chan durable.BridgedEvent, 64)
	go func() {
		if h.bridge == nil {
			replayOnly(ctx, h, ch)
			return
		}
		bridgeAndReplay(ctx, h, h.bridge, ch)
	}()
	return ch, nil
}

// Replay walks the workflow history and returns events from RunStep activity
// results after the given cursor.
func (h *handle) Replay(ctx context.Context, afterSeq int64) ([]durable.BridgedEvent, error) {
	iter := h.client.GetWorkflowHistory(ctx, h.id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var out []durable.BridgedEvent
	seq := afterSeq
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return out, fmt.Errorf("temporal: history %q: %w", h.id, err)
		}
		if event.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			continue
		}
		attrs := event.GetActivityTaskCompletedEventAttributes()
		if attrs == nil {
			continue
		}
		var outcome durable.StepOutcome
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &outcome); err != nil {
			continue
		}
		for _, ev := range outcome.Events {
			seq++
			if seq <= afterSeq {
				continue
			}
			out = append(out, durable.BridgedEvent{Seq: seq, Event: streaming.StreamEvent(ev)})
		}
	}
	return out, nil
}

func (h *handle) Signal(ctx context.Context, sig durable.Signal) error {
	name := sig.Name
	if name == "" {
		name = durable.SignalResume
	}
	return h.client.SignalWorkflow(ctx, h.id, "", name, sig)
}

func (h *handle) Cancel(ctx context.Context) error {
	if err := h.client.CancelWorkflow(ctx, h.id, ""); err != nil {
		return fmt.Errorf("temporal: cancel %q: %w", h.id, err)
	}
	return nil
}

func (h *handle) Result(ctx context.Context) (durable.RunResult, error) {
	var res durable.RunResult
	err := h.client.GetWorkflow(ctx, h.id, "").Get(ctx, &res)
	if err != nil {
		return durable.RunResult{}, fmt.Errorf("temporal: result %q: %w", h.id, err)
	}
	return res, nil
}

var (
	_ durable.Executor  = (*Executor)(nil)
	_ durable.RunHandle = (*handle)(nil)
)
