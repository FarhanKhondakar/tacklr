package temporal

import (
	"context"
	"testing"
	"time"

	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/streaming"
)

// searchAttributes registers the custom attributes the Executor sets on runs.
func searchAttributes() sdktemporal.SearchAttributes {
	return sdktemporal.NewSearchAttributes(
		sdktemporal.NewSearchAttributeKeyKeyword("TacklrSessionID").ValueSet(""),
		sdktemporal.NewSearchAttributeKeyKeyword("TacklrKind").ValueSet(""),
		sdktemporal.NewSearchAttributeKeyKeyword("TacklrWorker").ValueSet(""),
	)
}

// startDevServer boots a real Temporal dev server (downloaded CLI) and returns
// an executor + worker host sharing one client. Skipped in -short mode.
func startDevServer(t *testing.T, runner durable.StepRunner) (*testsuite.DevServer, *WorkerHost, *Executor) {
	t.Helper()
	ctx := context.Background()
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		LogLevel:         "warn",
		SearchAttributes: searchAttributes(),
	})
	if err != nil {
		t.Fatalf("start dev server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	client := srv.Client()
	queue := "tacklr-test"
	host := NewWorkerHostWithClient(client, queue, runner)
	if err := host.StartBackground(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(host.Close)
	return srv, host, host.Executor()
}

// TestTacklrRunWorkflow_completesThroughActivity is the flagship Temporal
// outcome: a run scheduled via the Executor executes the RunStep activity on
// the worker and its result is retrievable. This proves workflow + activity +
// executor wiring against a real Temporal service.
func TestTacklrRunWorkflow_completesThroughActivity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal integration in -short mode")
	}
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		return durable.StepOutcome{
			Complete: true,
			Output:   "temporal result",
			Events:   []streaming.StreamEvent{{Type: streaming.StreamEventMessage, Content: "tok"}},
		}, nil
	}
	_, _, exec := startDevServer(t, runner)

	ctx := context.Background()
	h, err := exec.Start(ctx, durable.RunSpec{
		ID:         "wf-complete-1",
		Kind:       durable.RunKindWorkerJob,
		SessionID:  "sess-1",
		WorkerName: "researcher",
		Task:       "dig",
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := h.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durable.StatusCompleted || res.Output != "temporal result" {
		t.Fatalf("result = %+v", res)
	}

	// Replay surfaces the step's recorded events from workflow history.
	events, err := h.Replay(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("expected replayed events from activity results")
	}
}

// TestTacklrRunWorkflow_interruptSignalResume proves a run parks on
// Interrupted, exposes the child interrupt ids, and resumes when the
// SignalResume signal arrives.
func TestTacklrRunWorkflow_interruptSignalResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal integration in -short mode")
	}
	var calls int
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		calls++
		if in.Kind == durable.StepStart {
			return durable.StepOutcome{
				Interrupted: &durable.InterruptState{ChildInterruptIDs: []string{"child-1"}},
			}, nil
		}
		return durable.StepOutcome{Complete: true, Output: "resumed output"}, nil
	}
	_, _, exec := startDevServer(t, runner)

	ctx := context.Background()
	h, err := exec.Start(ctx, durable.RunSpec{ID: "wf-interrupt-1", Kind: durable.RunKindWorkerJob, Task: "t"})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the workflow to park: the first activity completes and the
	// workflow blocks on the signal. Poll InterruptState until visible.
	var state *durable.InterruptState
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		state, _ = h.InterruptState(ctx)
		if state != nil && len(state.ChildInterruptIDs) == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if state == nil || state.ChildInterruptIDs[0] != "child-1" {
		t.Fatalf("interrupt state = %+v", state)
	}

	if err := h.Signal(ctx, durable.Signal{
		Name:        durable.SignalResume,
		Resolutions: map[string][]byte{"child-1": []byte(`{"ok":true}`)},
	}); err != nil {
		t.Fatal(err)
	}

	res, err := h.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durable.StatusCompleted || res.Output != "resumed output" {
		t.Fatalf("result = %+v", res)
	}
	if calls < 2 {
		t.Fatalf("expected start + resume steps, got %d", calls)
	}
}

// TestTacklrRunWorkflow_sessionTurnEndsOnInterrupt proves a session-turn run
// parks by ENDING with StatusInterrupted (resume is a new turn-scoped
// workflow), unlike a worker job which waits for a signal.
func TestTacklrRunWorkflow_sessionTurnEndsOnInterrupt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal integration in -short mode")
	}
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		return durable.StepOutcome{
			Interrupted: &durable.InterruptState{ChildInterruptIDs: []string{"c1"}},
		}, nil
	}
	_, _, exec := startDevServer(t, runner)

	ctx := context.Background()
	h, err := exec.Start(ctx, durable.RunSpec{
		ID:        "wf-turn-1",
		Kind:      durable.RunKindSessionTurn,
		SessionID: "sess-turn",
		AgentID:   "default",
		Task:      "ask",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durable.StatusInterrupted {
		t.Fatalf("session turn should end interrupted, got %+v", res)
	}
	// The run is terminal: resume is a new workflow, so Signaling the ended
	// run must error (no open workflow to receive it).
	if err := h.Signal(ctx, durable.Signal{Name: durable.SignalResume}); err == nil {
		t.Fatal("expected signal on an ended session-turn workflow to fail")
	}
}

// TestTacklrRunWorkflow_liveBridgeStreamsEvents proves the hybrid live +
// replay event bridge: with the executor created from the WorkerHost (same
// process), a step's events stream live through the bridge and are also
// recoverable via Replay with a consistent sequence cursor.
func TestTacklrRunWorkflow_liveBridgeStreamsEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal integration in -short mode")
	}
	release := make(chan struct{})
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		select {
		case <-ctx.Done():
			return durable.StepOutcome{}, ctx.Err()
		case <-release:
		}
		events := []streaming.StreamEvent{
			{Type: streaming.StreamEventMessage, Content: "first"},
			{Type: streaming.StreamEventMessage, Content: "second"},
		}
		// A real harness step forwards every event to the live sink (bridged in
		// this process) while also recording them for history replay.
		if sink := durable.EventSinkFromContext(ctx); sink != nil {
			for _, ev := range events {
				sink(ev)
			}
		}
		return durable.StepOutcome{
			Complete: true,
			Output:   "bridged",
			Events:   events,
		}, nil
	}
	_, _, exec := startDevServer(t, runner)

	ctx := context.Background()
	h, err := exec.Start(ctx, durable.RunSpec{
		ID:         "wf-bridge-1",
		Kind:       durable.RunKindWorkerJob,
		SessionID:  "sess-b",
		WorkerName: "researcher",
		Task:       "dig",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe before the step emits so live events are captured.
	events, err := h.Events(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var live []string
	done := make(chan struct{})
	go func() {
		for ev := range events {
			if ev.Event.Type == streaming.StreamEventMessage {
				live = append(live, ev.Event.Content)
			}
		}
		close(done)
	}()
	close(release)
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("live event stream never closed")
	}

	if len(live) != 2 || live[0] != "first" || live[1] != "second" {
		t.Fatalf("live events = %v", live)
	}
	// Replay agrees with the live stream (same events, same seq order).
	replayed, err := h.Replay(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || replayed[0].Event.Content != "first" || replayed[1].Event.Content != "second" {
		t.Fatalf("replay = %+v", replayed)
	}
}

// TestTacklrRunWorkflow_cancelTerminates proves CancelWorkflow surfaces as a
// terminal failure for the run.
func TestTacklrRunWorkflow_cancelTerminates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal integration in -short mode")
	}
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		if in.Kind == durable.StepStart {
			return durable.StepOutcome{Interrupted: &durable.InterruptState{ChildInterruptIDs: []string{"c"}}}, nil
		}
		select {
		case <-block:
		case <-ctx.Done():
			return durable.StepOutcome{}, ctx.Err()
		}
		return durable.StepOutcome{Complete: true}, nil
	}
	_, _, exec := startDevServer(t, runner)

	ctx := context.Background()
	h, err := exec.Start(ctx, durable.RunSpec{ID: "wf-cancel-1", Kind: durable.RunKindWorkerJob, Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the park so the workflow is alive and waiting.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if st, _ := h.InterruptState(ctx); st != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := h.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected cancelled workflow result to return an error")
	}
}
