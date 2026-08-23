// Package memory provides an in-process durable.Executor. It is the default
// when no external backend (Temporal, Azure, AWS) is wired, and it is the
// test double for durable-run logic. It runs the StepRunner on a goroutine,
// bridges events, and supports Signal/Cancel in-process. It is intentionally
// not crash-durable; durability across restarts requires a real backend.
package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Executor is an in-process durable.Executor.
type Executor struct {
	runner durable.StepRunner

	mu      sync.Mutex
	handles map[string]*handle
}

// New returns an Executor that runs steps with runner. Runner must be non-nil.
func New(runner durable.StepRunner) (*Executor, error) {
	if runner == nil {
		return nil, fmt.Errorf("memory: StepRunner is required")
	}
	return &Executor{
		runner:  runner,
		handles: make(map[string]*handle),
	}, nil
}

// Must returns an Executor or panics. For construction in init/test paths.
func Must(runner durable.StepRunner) *Executor {
	e, err := New(runner)
	if err != nil {
		panic(err)
	}
	return e
}

// Start begins a run and returns its handle. Re-Starting an existing ID
// attaches to the same handle.
func (e *Executor) Start(ctx context.Context, spec durable.RunSpec) (durable.RunHandle, error) {
	if spec.ID == "" {
		return nil, fmt.Errorf("memory: run id is required: %w", durable.ErrRunNotFound)
	}
	e.mu.Lock()
	if h, ok := e.handles[spec.ID]; ok {
		e.mu.Unlock()
		return h, nil
	}
	h := newHandle(spec)
	e.handles[spec.ID] = h
	e.mu.Unlock()

	go e.run(h)
	return h, nil
}

func (e *Executor) run(h *handle) {
	defer h.finish()
	ctx := context.Background()

	step := durable.StepInput{Spec: h.spec, Kind: durable.StepStart}
	if len(h.spec.Resolutions) > 0 {
		step.Kind = durable.StepResume
		step.Resolutions = h.spec.Resolutions
	}
	for {
		outcome, err := e.runner(ctx, step)
		if err != nil {
			h.fail(err)
			return
		}
		if outcome.Err != "" {
			h.fail(errors.New(outcome.Err))
			return
		}
		h.recordEvents(outcome.Events)
		if outcome.Complete {
			h.complete(outcome.Output)
			return
		}
		if outcome.Interrupted == nil {
			h.fail(errors.New("memory: step returned neither Complete nor Interrupted"))
			return
		}
		// A session turn parks for user input, but its resume is a NEW run
		// (the registry reconstructs from the checkpoint each RunTurn). End the
		// run with StatusInterrupted; a resume starts a fresh run whose first
		// step is StepResume.
		if h.spec.Kind == durable.RunKindSessionTurn {
			h.finishInterrupted(outcome.Interrupted)
			return
		}
		h.setInterrupted(outcome.Interrupted)

		select {
		case <-h.done:
			return
		case res := <-h.resumeCh:
			step = durable.StepInput{Spec: h.spec, Kind: durable.StepResume, Resolutions: res}
			h.mu.Lock()
			h.status = durable.StatusRunning
			h.parked = nil
			h.mu.Unlock()
		}
	}
}

// handle implements durable.RunHandle for an in-process run.
type handle struct {
	spec durable.RunSpec

	mu       sync.Mutex
	status   durable.Status
	result   durable.RunResult
	events   []durable.BridgedEvent
	seq      int64
	subs     []chan durable.BridgedEvent
	done     chan struct{}
	resumeCh chan map[string][]byte
	// parked holds the interrupt state while status is StatusInterrupted.
	parked *durable.InterruptState
}

func newHandle(spec durable.RunSpec) *handle {
	return &handle{
		spec:     spec,
		status:   durable.StatusRunning,
		done:     make(chan struct{}),
		resumeCh: make(chan map[string][]byte, 1),
	}
}

func (h *handle) ID() string { return h.spec.ID }

func (h *handle) Status(_ context.Context) (durable.Status, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status, nil
}

func (h *handle) InterruptState(_ context.Context) (*durable.InterruptState, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.status != durable.StatusInterrupted {
		return nil, nil
	}
	if h.parked == nil {
		return nil, nil
	}
	cp := *h.parked
	return &cp, nil
}

func (h *handle) Events(_ context.Context) (<-chan durable.BridgedEvent, error) {
	ch := make(chan durable.BridgedEvent, 64)
	h.mu.Lock()
	history := make([]durable.BridgedEvent, len(h.events))
	copy(history, h.events)
	terminal := h.status == durable.StatusCompleted || h.status == durable.StatusFailed
	if !terminal {
		h.subs = append(h.subs, ch)
	}
	h.mu.Unlock()

	go func() {
		for _, ev := range history {
			ch <- ev
		}
		if terminal {
			close(ch)
		}
	}()
	return ch, nil
}

func (h *handle) Signal(_ context.Context, sig durable.Signal) error {
	if sig.Name != "" && sig.Name != durable.SignalResume {
		return fmt.Errorf("memory: unsupported signal %q", sig.Name)
	}
	h.mu.Lock()
	if h.status == durable.StatusCompleted || h.status == durable.StatusFailed {
		h.mu.Unlock()
		return fmt.Errorf("%w: %s", durable.ErrRunTerminal, h.spec.ID)
	}
	if h.status != durable.StatusInterrupted {
		h.mu.Unlock()
		return fmt.Errorf("memory: run %s not awaiting resume", h.spec.ID)
	}
	h.mu.Unlock()

	select {
	case h.resumeCh <- sig.Resolutions:
		return nil
	case <-h.done:
		return fmt.Errorf("%w: %s", durable.ErrRunTerminal, h.spec.ID)
	}
}

func (h *handle) Cancel(_ context.Context) error {
	h.mu.Lock()
	if h.status == durable.StatusCompleted || h.status == durable.StatusFailed {
		h.mu.Unlock()
		return nil
	}
	h.status = durable.StatusFailed
	h.result = durable.RunResult{Status: durable.StatusFailed, Err: context.Canceled}
	// Close done so a run parked on resumeCh unblocks and Result resolves.
	select {
	case <-h.done:
	default:
		close(h.done)
	}
	h.mu.Unlock()
	return nil
}

func (h *handle) Result(ctx context.Context) (durable.RunResult, error) {
	select {
	case <-ctx.Done():
		return durable.RunResult{}, ctx.Err()
	case <-h.done:
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.result, nil
}

func (h *handle) Replay(_ context.Context, afterSeq int64) ([]durable.BridgedEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]durable.BridgedEvent, 0, len(h.events))
	for _, ev := range h.events {
		if ev.Seq > afterSeq {
			out = append(out, ev)
		}
	}
	return out, nil
}

// --- internal state transitions ---

func (h *handle) setInterrupted(state *durable.InterruptState) {
	h.mu.Lock()
	h.status = durable.StatusInterrupted
	h.parked = state
	h.mu.Unlock()
}

func (h *handle) recordEvents(events []streaming.StreamEvent) {
	if len(events) == 0 {
		return
	}
	h.mu.Lock()
	for _, ev := range events {
		h.seq++
		bridged := durable.BridgedEvent{Seq: h.seq, Event: ev}
		h.events = append(h.events, bridged)
		for _, sub := range h.subs {
			select {
			case sub <- bridged:
			default:
			}
		}
	}
	h.mu.Unlock()
}

func (h *handle) complete(output string) {
	h.mu.Lock()
	h.status = durable.StatusCompleted
	h.result = durable.RunResult{Status: durable.StatusCompleted, Output: output}
	h.mu.Unlock()
}

// finishInterrupted ends a session-turn run parked for input. The run is
// terminal (resume is a new run), so Status and Result resolve to
// StatusInterrupted and event subscribers are closed.
func (h *handle) finishInterrupted(state *durable.InterruptState) {
	h.mu.Lock()
	h.status = durable.StatusInterrupted
	h.parked = state
	h.result = durable.RunResult{Status: durable.StatusInterrupted}
	select {
	case <-h.done:
	default:
		close(h.done)
	}
	for _, sub := range h.subs {
		close(sub)
	}
	h.subs = nil
	h.mu.Unlock()
}

func (h *handle) fail(err error) {
	h.mu.Lock()
	if h.status == durable.StatusCompleted || h.status == durable.StatusFailed {
		h.mu.Unlock()
		return
	}
	h.status = durable.StatusFailed
	h.result = durable.RunResult{Status: durable.StatusFailed, Err: err}
	h.mu.Unlock()
}

func (h *handle) finish() {
	h.mu.Lock()
	select {
	case <-h.done:
	default:
		close(h.done)
	}
	for _, sub := range h.subs {
		close(sub)
	}
	h.subs = nil
	h.mu.Unlock()
}

var (
	_ durable.Executor  = (*Executor)(nil)
	_ durable.RunHandle = (*handle)(nil)
)
