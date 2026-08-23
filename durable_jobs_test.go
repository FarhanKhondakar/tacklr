package tacklr

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/memory"
	"github.com/ryanaldo34/tacklr/stores"
)

// TestDurableBackgroundJobs_runsThroughExecutor proves that with
// AgentOptions.Durable set, an async spawn_worker job is driven by the
// durable Executor (not the in-process goroutine) and its result is
// collectable via get_job. The memory executor is in-process, so this is a
// true integration test of the executor wiring with no external service.
func TestDurableBackgroundJobs_runsThroughExecutor(t *testing.T) {
	// Arrange: a worker model that finishes immediately, and a factory that
	// rebuilds the worker harness for each durable step.
	workerModel := &mockStrategy{
		invokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "durable research done", IsComplete: true}
		},
	}
	store := stores.NewInMemoryStore()
	factory := func(name, id string) (AgentOptions, error) {
		if name != "researcher" {
			return AgentOptions{}, ErrNotFound
		}
		return AgentOptions{
			Config: Config{MaxWindowSize: 8192},
			Model:  workerModel,
			Store:  store,
		}, nil
	}
	runner, err := DurableStepRunner(factory)
	if err != nil {
		t.Fatal(err)
	}
	executor := memory.Must(runner)

	step := 0
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bg1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"dig","block":false}`),
			}, IsComplete: true}
		case 2:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("g1", "get_job", `{"job_id":"bg1","block":true}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
		}
	}

	h := mustNewAgent(t, AgentOptions{
		Config:    Config{MaxWindowSize: 8192},
		Model:     parent,
		Store:     store,
		Durable:   executor,
		SubAgents: []*SubAgent{{WorkerName: "researcher", Model: workerModel}},
	})
	defer h.Close()

	// Act
	got := drainEvents(mustRun(t, h, "research in the background"))

	// Assert
	if out := toolResultByName(got, "spawn_worker"); !strings.Contains(out, "Job bg1 scheduled") {
		t.Fatalf("spawn = %q", out)
	}
	if lastToolResultByName(got, "get_job") != "durable research done" {
		t.Fatalf("blocking get_job = %q", lastToolResultByName(got, "get_job"))
	}
	if hasEventType(got, StreamEventError) {
		t.Fatalf("unexpected error: %+v", summarizeEvents(got))
	}
	// The job must be recorded as a durable run (not an in-process worker).
	if j := h.getJob("bg1"); j != nil && j.durable == nil && j.worker != nil {
		t.Fatalf("expected durable job, got in-process worker run")
	}
}

// TestDurableBackgroundJobs_cancelRoutesToExecutor proves cancel_job cancels
// the durable run through the Executor.
func TestDurableBackgroundJobs_cancelRoutesToExecutor(t *testing.T) {
	release := make(chan struct{})
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "unreachable", IsComplete: true}
		},
	}
	store := stores.NewInMemoryStore()
	factory := func(name, id string) (AgentOptions, error) {
		return AgentOptions{Config: Config{MaxWindowSize: 8192}, Model: workerModel, Store: store}, nil
	}
	executor := memory.Must(mustRunner(t, factory))

	step := 0
	var h *AgentHarness
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bg1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"dig","block":false}`),
			}, IsComplete: true}
		case 2:
			waitJobStatus(t, h, "bg1", jobStatusRunning)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("c1", "cancel_job", `{"job_id":"bg1"}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		Config:    Config{MaxWindowSize: 8192},
		Model:     parent,
		Store:     store,
		Durable:   executor,
		SubAgents: []*SubAgent{{WorkerName: "researcher", Model: workerModel}},
	})
	defer h.Close()

	got := drainEvents(mustRun(t, h, "start then cancel"))
	if out := toolResultByName(got, "cancel_job"); !strings.Contains(out, "cancelled") {
		t.Fatalf("cancel_job = %q", out)
	}
	if j := h.getJob("bg1"); j != nil {
		t.Fatalf("cancelled job should be removed, still present")
	}
}

func mustRunner(t *testing.T, factory DurableStepRunnerFactory) durable.StepRunner {
	t.Helper()
	r, err := DurableStepRunner(factory)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestDurableBackgroundJobs_interruptResume proves the full durable interrupt
// cycle: a worker calls ask_user_choice, the run parks on the durable
// Executor, get_job with block=true resolves it via Signal, and the resumed
// step completes. The worker harness is rebuilt from its checkpoint on resume,
// which is the crash-recovery path.
func TestDurableBackgroundJobs_interruptResume(t *testing.T) {
	// Worker: turn 1 asks a question (parks), turn 2 (after resume) answers.
	var workerCalls int
	workerModel := &mockStrategy{}
	workerModel.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		workerCalls++
		if workerCalls == 1 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("ask1", "ask_user_choice", `{"question":"pick one","choices":[{"title":"a"},{"title":"b"}]}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "worker answered", IsComplete: true}
	}

	store := stores.NewInMemoryStore()
	factory := func(name, id string) (AgentOptions, error) {
		return AgentOptions{Config: Config{MaxWindowSize: 8192}, Model: workerModel, Store: store}, nil
	}
	executor := memory.Must(mustRunner(t, factory))

	step := 0
	var h *AgentHarness
	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		step++
		switch step {
		case 1:
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bg1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"ask","block":false}`),
			}, IsComplete: true}
		case 2:
			waitJobStatus(t, h, "bg1", jobStatusInterrupted)
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("g1", "get_job", `{"job_id":"bg1","block":true}`),
			}, IsComplete: true}
		default:
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
		}
	}

	h = mustNewAgent(t, AgentOptions{
		Config:    Config{MaxWindowSize: 8192},
		Model:     parent,
		Store:     store,
		Durable:   executor,
		SubAgents: []*SubAgent{{WorkerName: "researcher", Model: workerModel}},
	})
	defer h.Close()

	// Run the turn; it will park on the get_job interrupt (the durable job is
	// awaiting user input). Drain until the interrupt surfaces.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := h.Run(ctx, "run durable job that asks")
	if err != nil {
		t.Fatal(err)
	}
	var sawInterrupt bool
	for ev := range events {
		if ev.Type == StreamEventInterrupt {
			sawInterrupt = true
		}
		if ev.Type == StreamEventError {
			t.Fatalf("turn error: %v", ev.Error)
		}
	}
	if !sawInterrupt {
		t.Fatal("expected the durable job to interrupt the parent turn")
	}

	// Resolve the parked get_job interrupt (tool-call id "g1"). The payload is
	// the consumer's answer to the worker's question; the placeholder ignores
	// it and the harness stashes it, then resumeDurableJob forwards it to the
	// worker's child interrupt id via the Signal.
	resumePayload := []byte(`{"interruptId":"g1","selectionIdx":1}`)
	resumed, err := h.ReturnFromInterrupt(context.Background(), map[string][]byte{
		"g1": resumePayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drainEvents(resumed)
	if out := lastToolResultByName(got, "get_job"); out != "worker answered" {
		t.Fatalf("resumed get_job = %q, events=%v", out, summarizeEvents(got))
	}
	if hasEventType(got, StreamEventError) {
		t.Fatalf("unexpected error after resume: %+v", summarizeEvents(got))
	}
}

// TestDurableBackgroundJobs_surviveClose proves the core durability guarantee:
// closing the parent harness does not cancel a durable job. The backend owns
// the run lifecycle, so the run keeps going and its result is still retrievable.
func TestDurableBackgroundJobs_surviveClose(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
			select {
			case <-started:
			default:
				close(started)
			}
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "finished after close", IsComplete: true}
		},
	}
	store := stores.NewInMemoryStore()
	factory := func(name, id string) (AgentOptions, error) {
		return AgentOptions{Config: Config{MaxWindowSize: 8192}, Model: workerModel, Store: store}, nil
	}
	executor := memory.Must(mustRunner(t, factory))

	parent := &mockStrategy{}
	parent.invokeFn = func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
		if parent.callNum.Load() == 1 {
			ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
				toolCall("bg1", "spawn_worker", `{"worker_name":"researcher","task_description_and_context":"hang","block":false}`),
			}, IsComplete: true}
			return
		}
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "parent done", IsComplete: true}
	}

	h := mustNewAgent(t, AgentOptions{
		Config:    Config{MaxWindowSize: 8192},
		Model:     parent,
		Store:     store,
		Durable:   executor,
		SubAgents: []*SubAgent{{WorkerName: "researcher", Model: workerModel}},
	})

	// Run in a cancellable context; the parent turn will not finish while the
	// background job runs, so drain in a goroutine and cancel once started.
	ctx, cancel := context.WithCancel(context.Background())
	events, err := h.Run(ctx, "schedule a durable job")
	if err != nil {
		t.Fatal(err)
	}
	drained := make(chan struct{})
	go func() {
		_ = drainEvents(events)
		close(drained)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("durable worker never started")
	}
	cancel()
	<-drained

	// Close the parent harness: the durable run must NOT be cancelled.
	h.Close()

	// The backend run is still alive and completes once released.
	close(release)
	handle, err := executor.Start(context.Background(), durable.RunSpec{ID: "bg1"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := handle.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durable.StatusCompleted || res.Output != "finished after close" {
		t.Fatalf("durable run after parent Close = %+v", res)
	}
}
