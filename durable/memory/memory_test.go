package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/streaming"
)

func TestExecutor_StartCompletesRun(t *testing.T) {
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		return durable.StepOutcome{
			Complete: true,
			Output:   "done",
			Events:   []streaming.StreamEvent{{Type: streaming.StreamEventMessage, Content: "hi"}},
		}, nil
	}
	exec := Must(runner)

	h, err := exec.Start(context.Background(), durable.RunSpec{ID: "r1", Kind: durable.RunKindWorkerJob, WorkerName: "w", Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durable.StatusCompleted || res.Output != "done" {
		t.Fatalf("result = %+v", res)
	}
	// Replaying history returns the step's events.
	events, err := h.Replay(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Event.Content != "hi" {
		t.Fatalf("replay = %+v", events)
	}
	// Re-starting the same ID attaches to the finished run.
	again, err := exec.Start(context.Background(), durable.RunSpec{ID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID() != h.ID() {
		t.Fatal("re-start should attach to existing run")
	}
}

func TestExecutor_InterruptResume(t *testing.T) {
	started := make(chan struct{})
	resumed := make(chan map[string][]byte, 1)
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		if in.Kind == durable.StepStart {
			close(started)
			return durable.StepOutcome{
				Interrupted: &durable.InterruptState{ChildInterruptIDs: []string{"child-1"}},
			}, nil
		}
		resumed <- in.Resolutions
		return durable.StepOutcome{Complete: true, Output: "resumed"}, nil
	}
	exec := Must(runner)

	h, err := exec.Start(context.Background(), durable.RunSpec{ID: "r2", Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	// Wait for the run to park.
	waitForStatus(t, h, durable.StatusInterrupted)

	state, err := h.InterruptState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || len(state.ChildInterruptIDs) != 1 || state.ChildInterruptIDs[0] != "child-1" {
		t.Fatalf("interrupt state = %+v", state)
	}

	if err := h.Signal(context.Background(), durable.Signal{
		Name:        durable.SignalResume,
		Resolutions: map[string][]byte{"child-1": []byte(`{"ok":true}`)},
	}); err != nil {
		t.Fatal(err)
	}
	got := <-resumed
	if string(got["child-1"]) != `{"ok":true}` {
		t.Fatalf("resolutions = %v", got)
	}
	res, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durable.StatusCompleted || res.Output != "resumed" {
		t.Fatalf("result = %+v", res)
	}
}

func TestExecutor_CancelTerminates(t *testing.T) {
	block := make(chan struct{})
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		if in.Kind == durable.StepStart {
			return durable.StepOutcome{Interrupted: &durable.InterruptState{ChildInterruptIDs: []string{"c"}}}, nil
		}
		<-block
		return durable.StepOutcome{Complete: true}, nil
	}
	exec := Must(runner)

	h, err := exec.Start(context.Background(), durable.RunSpec{ID: "r3", Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, h, durable.StatusInterrupted)
	if err := h.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	res, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durable.StatusFailed || !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("result = %+v", res)
	}
	close(block)
}

func TestExecutor_SignalOnRunningRunFails(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
		<-release
		return durable.StepOutcome{Complete: true, Output: "x"}, nil
	}
	exec := Must(runner)
	h, err := exec.Start(context.Background(), durable.RunSpec{ID: "r4", Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Signal(context.Background(), durable.Signal{Name: durable.SignalResume}); err == nil {
		t.Fatal("signal on running (non-interrupted) run should fail")
	}
}

func waitForStatus(t *testing.T, h durable.RunHandle, want durable.Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, err := h.Status(context.Background())
		if err == nil && st == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run did not reach status %v", want)
}
