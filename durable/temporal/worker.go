package temporal

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr/durable"
)

// WorkerHost hosts the Temporal worker that executes TacklrRunWorkflow and
// the RunStep activity. It shares a client with the Executor so a host
// process is both the scheduler (Executor) and the executor (worker) of
// durable runs — the in-process deployment that keeps live event bridging
// possible. Deploying the worker separately degrades event delivery to
// history replay.
type WorkerHost struct {
	client client.Client
	worker worker.Worker
	queue  string
	bridge *eventBridge
}

// NewWorkerHost dials Temporal, builds the worker, and registers the
// TacklrRunWorkflow plus the RunStep activity backed by runner. runner is
// required. Call Start to begin polling and Close to stop.
func NewWorkerHost(ctx context.Context, cfg Config, runner durable.StepRunner) (*WorkerHost, error) {
	if runner == nil {
		return nil, fmt.Errorf("temporal: StepRunner is required")
	}
	opts, err := cfg.ClientOptions()
	if err != nil {
		return nil, err
	}
	c, err := client.DialContext(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("temporal: dial: %w", err)
	}
	return NewWorkerHostWithClient(c, cfg.TaskQueueName(), runner), nil
}

// NewWorkerHostWithClient registers the workflow and activity on a worker
// over an existing client (shared with the Executor). The host owns the
// live event bridge so Executor handles created from it stream events live.
func NewWorkerHostWithClient(c client.Client, taskQueue string, runner durable.StepRunner) *WorkerHost {
	b := newEventBridge()
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(TacklrRunWorkflow)
	w.RegisterActivityWithOptions(runStepActivity(runner, b, c), activity.RegisterOptions{Name: ActivityRunStep})
	return &WorkerHost{client: c, worker: w, queue: taskQueue, bridge: b}
}

// Executor returns an Executor over the same client, for hosts that want one
// process to both schedule and execute durable runs. Handles from this
// executor stream events through the host's live bridge.
func (h *WorkerHost) Executor() *Executor {
	e := NewExecutorWithClient(h.client, h.queue)
	e.bridge = h.bridge
	return e
}

// Start begins polling the task queue. It blocks until the worker stops or
// the interrupt channel fires, mirroring worker.Run semantics.
func (h *WorkerHost) Start() error {
	return h.worker.Run(worker.InterruptCh())
}

// StartBackground begins polling without blocking, for embedding in a server
// that manages its own lifecycle.
func (h *WorkerHost) StartBackground() error {
	return h.worker.Start()
}

// Close stops the worker and releases the client.
func (h *WorkerHost) Close() {
	h.worker.Stop()
	h.client.Close()
}
