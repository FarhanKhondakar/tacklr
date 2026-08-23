// Package durable defines the provider-agnostic durable-run abstraction for
// the agent harness. A durable Executor drives a run of a worker job or a
// session turn; the harness and server depend only on this interface, never
// on a concrete engine.
//
// Backends:
//   - durable/memory    in-process, same-process lifecycle (the default when
//     no durable backend is wired)
//   - durable/temporal   Temporal.io durable execution
//   - durable/azure / durable/aws   (future; raise-event / task-token map to Signal)
//
// The abstraction is Tacklr-shaped, not a generic workflow API: each backend
// adapts engine primitives (Temporal signals, Azure raise-event, AWS task
// tokens) onto Signal/Cancel/Replay.
package durable

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr/streaming"
)

// RunKind identifies the shape of a durable run.
type RunKind int

const (
	// RunKindWorkerJob is a background spawn_worker job (block=false).
	RunKindWorkerJob RunKind = iota
	// RunKindSessionTurn is a session turn (prompt / resume) driven durably.
	RunKindSessionTurn
)

func (k RunKind) String() string {
	switch k {
	case RunKindWorkerJob:
		return "worker_job"
	case RunKindSessionTurn:
		return "session_turn"
	default:
		return "unknown"
	}
}

// Status is the terminal-or-waiting state of a durable run.
type Status int

const (
	StatusRunning Status = iota
	// StatusInterrupted means the run is parked awaiting user input; it
	// resumes when the host delivers a SignalResume.
	StatusInterrupted
	StatusCompleted
	StatusFailed
)

func (s Status) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusInterrupted:
		return "interrupted"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// RunSpec describes the work to start. It is the only payload an Executor
// needs; backends map it onto their own workflow/execution input.
type RunSpec struct {
	// ID is the stable, unique run ID (job id or sessionID#turn). Backends use
	// it as the workflow/execution id so re-start is deduplicated.
	ID string
	// Kind distinguishes worker jobs from session turns.
	Kind RunKind
	// SessionID is the harness session/checkpoint id to create or resume.
	SessionID string
	// WorkerName is the subagent spec name for worker jobs.
	WorkerName string
	// Task is the user prompt / worker task.
	Task string
	// Load is true when the run resumes an existing checkpointed session.
	Load bool
	// Resolutions carries interrupt resolutions (tool-call id -> payload) when
	// the run is started as a resume.
	Resolutions map[string][]byte
}

// Signal resumes or steers a waiting run.
type Signal struct {
	// Name selects the signal; for interrupt resolution use SignalResume.
	Name string
	// Resolutions maps tool-call id to the consumer payload for that interrupt.
	Resolutions map[string][]byte
}

// RunResult is the terminal outcome of a run.
type RunResult struct {
	Status Status
	// Output is the final result when Status is StatusCompleted.
	Output string
	// Err is the terminal failure when Status is StatusFailed.
	Err error
}

// BridgedEvent wraps a harness event with a monotonic cursor so consumers can
// replay history and switch to the live stream without loss or duplicates.
type BridgedEvent struct {
	Seq   int64
	Event streaming.StreamEvent
}

// RunHandle is the live handle to a started run.
type RunHandle interface {
	ID() string
	// Status reports the current run state (non-blocking).
	Status(ctx context.Context) (Status, error)
	// InterruptState returns the park state when Status is StatusInterrupted,
	// including the child interrupt ids a resume Signal must target. It
	// returns nil when the run is not interrupted.
	InterruptState(ctx context.Context) (*InterruptState, error)
	// Events returns the live event stream; it closes at a terminal state.
	Events(ctx context.Context) (<-chan BridgedEvent, error)
	// Signal delivers an interrupt resolution (or other steer) to the run.
	Signal(ctx context.Context, sig Signal) error
	// Cancel requests cancellation of the run.
	Cancel(ctx context.Context) error
	// Result blocks until the run reaches a terminal state.
	Result(ctx context.Context) (RunResult, error)
	// Replay returns all events after the given sequence cursor, for
	// crash/disconnect recovery.
	Replay(ctx context.Context, afterSeq int64) ([]BridgedEvent, error)
}

// Executor starts durable runs. A nil Executor on the harness/server means
// in-process behavior; durable/memory provides an in-process implementation
// used by tests and as the shipped default.
type Executor interface {
	// Start begins a durable run and returns its handle. Starting a run with
	// an ID that already exists attaches to the existing run (idempotent).
	Start(ctx context.Context, spec RunSpec) (RunHandle, error)
}

// SignalResume is the signal name used to resolve interrupts and resume.
const SignalResume = "tacklr.resume"

// ErrRunNotFound is returned when a handle/operation targets an unknown run.
var ErrRunNotFound = errors.New("durable: run not found")

// ErrRunTerminal is returned when an operation targets a finished run.
var ErrRunTerminal = errors.New("durable: run already terminal")

// StepKind identifies the phase of one RunStep activity invocation.
type StepKind int

const (
	StepStart  StepKind = iota // start a new run
	StepResume                 // resume from a parked interrupt
)

// StepInput is the input to a single RunStep activity. Backends run it as a
// heartbeat-enabled activity; the step rebuilds the harness from the last
// checkpoint so a retry after a crash is idempotent.
type StepInput struct {
	Spec        RunSpec
	Kind        StepKind
	Resolutions map[string][]byte
}

// StepOutcome is the result of one RunStep activity. Exactly one of Complete
// or Interrupted is set on success; Err is set on failure.
type StepOutcome struct {
	Complete bool
	// Output is the final result when Complete is true.
	Output string
	// Interrupted is the park state when the run awaits user input.
	Interrupted *InterruptState
	// Events are the harness events produced by this step (capped).
	Events []streaming.StreamEvent
	// Err is the terminal failure for this step.
	Err string
}

// InterruptState is the durable park state returned when a run awaits input.
type InterruptState struct {
	// ChildInterruptIDs are the pending child interrupt ids to resolve.
	ChildInterruptIDs []string
	// Payload is the serialized primary interrupt (for display/resume).
	Payload []byte
	// WorkerSessionID is the parked worker session id, when set.
	WorkerSessionID string
}

// StepRunner executes one RunStep against the harness. Hosts inject this so
// durable backends (Temporal, Azure, AWS) never import the harness package;
// the closure rebuilds AgentHarness from a checkpoint and runs one turn.
//
// A correct StepRunner must:
//   - rebuild the harness from spec.SessionID's checkpoint when Kind is
//     StepResume (or Load), else construct a fresh harness
//   - run Run/ReturnFromInterrupt and drain events into outcome.Events
//   - persist the session checkpoint (the harness does this on exit)
//   - return StepOutcome{Complete|Interrupted|Err}
type StepRunner func(ctx context.Context, in StepInput) (StepOutcome, error)
