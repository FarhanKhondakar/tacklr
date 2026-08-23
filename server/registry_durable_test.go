package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable/memory"
	"github.com/ryanaldo34/tacklr/streaming"
)

// durableTestRegistry builds a registry whose turns run through a durable
// Executor (in-process memory) backed by the registry's own turn StepRunner.
func durableTestRegistry(t *testing.T, strategy tacklr.InferenceStrategy) (*Registry, *memory.Executor) {
	t.Helper()
	store := testStore(t)
	r := newTestRegistry(store, strategy, nil)
	runner, err := r.SessionTurnStepRunner()
	if err != nil {
		t.Fatal(err)
	}
	executor := memory.Must(runner)
	r.durable = executor
	return r, executor
}

// TestRunTurn_durablePromptCompletes proves a session turn started through a
// durable executor streams events and completes, with the harness rebuilt
// inside the step runner from the agent spec.
func TestRunTurn_durablePromptCompletes(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "durable turn reply", IsComplete: true}
		},
	}
	r, _ := durableTestRegistry(t, strategy)

	stream, err := r.RunTurn(context.Background(), TurnRequest{
		SessionID: "thread-1",
		AgentID:   "default",
		Prompt:    "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	var events []streaming.StreamEvent
	for ev := range stream.Events {
		events = append(events, ev)
	}
	stream.Cancel()
	stream.Close()

	var sawComplete, sawMessage bool
	for _, ev := range events {
		if ev.Type == streaming.StreamEventComplete {
			sawComplete = true
		}
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "durable turn reply") {
			sawMessage = true
		}
	}
	if !sawMessage || !sawComplete {
		t.Fatalf("events = %+v", summarize(events))
	}
	if stream.SessionID() != "thread-1" {
		t.Fatalf("session id = %q", stream.SessionID())
	}
}

// TestRunTurn_durableInterruptResumeAsNewWorkflow proves a durable turn that
// parks (interrupt) ends the workflow, and a follow-on resume turn carrying
// the resolution starts a new workflow and completes.
func TestRunTurn_durableInterruptResumeAsNewWorkflow(t *testing.T) {
	step := 0
	strategy := &mockInferenceStrategy{}
	strategy.invokeFn = func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		step++
		if step == 1 {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
				{ID: "tc1", CallID: "tc1", Name: "ask_user_choice", Arguments: `{"question":"pick","choices":[{"title":"a"},{"title":"b"}]}`},
			}, IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after resume", IsComplete: true}
	}
	r, _ := durableTestRegistry(t, strategy)

	// First turn parks on the interrupt.
	stream, err := r.RunTurn(context.Background(), TurnRequest{
		SessionID: "thread-2",
		AgentID:   "default",
		Prompt:    "ask",
	})
	if err != nil {
		t.Fatal(err)
	}
	var first []streaming.StreamEvent
	for ev := range stream.Events {
		first = append(first, ev)
	}
	stream.Close()
	var sawInterrupt bool
	for _, ev := range first {
		if ev.Type == streaming.StreamEventInterrupt {
			sawInterrupt = true
		}
	}
	if !sawInterrupt {
		t.Fatalf("expected interrupt, events=%+v", summarize(first))
	}

	// Resume as a new turn workflow carrying the resolution.
	resumed, err := r.RunTurn(context.Background(), TurnRequest{
		SessionID: "thread-2",
		AgentID:   "default",
		Load:      true,
		Responses: map[string]json.RawMessage{"tc1": json.RawMessage(`{"interruptId":"tc1","selectionIdx":1}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var second []streaming.StreamEvent
	for ev := range resumed.Events {
		second = append(second, ev)
	}
	resumed.Cancel()
	resumed.Close()

	var sawAfter, sawComplete bool
	for _, ev := range second {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "after resume") {
			sawAfter = true
		}
		if ev.Type == streaming.StreamEventComplete {
			sawComplete = true
		}
	}
	if !sawAfter || !sawComplete {
		t.Fatalf("resumed events = %+v", summarize(second))
	}
}

// TestRunTurn_durableCancelCancelsWorkflow proves EventStream.Cancel cancels
// the backing durable run and the stream closes.
func TestRunTurn_durableCancelCancelsWorkflow(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	strategy := &mockInferenceStrategy{}
	strategy.invokeFn = func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		select {
		case <-block:
		case <-ctx.Done():
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "unreachable", IsComplete: true}
	}
	r, _ := durableTestRegistry(t, strategy)

	stream, err := r.RunTurn(context.Background(), TurnRequest{SessionID: "thread-3", AgentID: "default", Prompt: "block"})
	if err != nil {
		t.Fatal(err)
	}
	stream.Cancel()
	done := make(chan struct{})
	go func() {
		for range stream.Events {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled durable turn stream never closed")
	}
	stream.Close()
}

func summarize(events []streaming.StreamEvent) string {
	var b strings.Builder
	for _, ev := range events {
		b.WriteString(string(ev.Type))
		b.WriteString(":")
		b.WriteString(ev.Content)
		b.WriteString(" ")
	}
	return b.String()
}
