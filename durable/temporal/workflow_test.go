package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"path/filepath"
	"sync"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestSessionWorkflow_inferenceRefusedFailsTurn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	var attempts atomic.Int32
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		attempts.Add(1)
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Error: tacklr.ErrModelRefused}
	}),
		MaxWindowSize: 8192}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))

	id := durable.SessionID("sess-model-refused")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, ActivityAttempts: 3})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("refusal attempts = %d, want 3", n)
	}
	st := querySession(t, env)
	if st.State != durable.SessionFailed || st.Waiting {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventError && (errors.Is(ev.Error, tacklr.ErrModelRefused) ||
			strings.Contains(ev.Content, tacklr.ErrModelRefused.Error()) ||
			strings.Contains(ev.Fail, tacklr.ErrModelRefused.Error())) {
			saw = true
		}
	}
	if !saw {
		t.Fatal("want StreamEventError with model refused")
	}
}

func TestSessionWorkflow_permanentInferenceDoesNotRetry(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	var attempts atomic.Int32
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		attempts.Add(1)
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Error: tacklr.ErrApiKeyNotSet, Content: tacklr.ErrApiKeyNotSet.Error()}
	}),
		MaxWindowSize: 8192}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))

	id := durable.SessionID("sess-permanent")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id, ActivityAttempts: 5})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("permanent attempts = %d, want 1", n)
	}
	st := querySession(t, env)
	if st.State != durable.SessionFailed || st.Waiting {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventError && strings.Contains(ev.Fail, tacklr.ErrApiKeyNotSet.Error()) {
			saw = true
			if strings.Count(ev.Fail, tacklr.ErrApiKeyNotSet.Error()) != 1 {
				t.Fatalf("failure text repeated: %q", ev.Fail)
			}
		}
	}
	if !saw {
		t.Fatal("want StreamEventError with api key not set")
	}
}

func TestSessionWorkflow_activityRetryThenCompletes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	var attempts atomic.Int32
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if attempts.Add(1) == 1 {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Error: tacklr.Network(errors.New("transient"))}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-retry", IsComplete: true}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	fallback := &retryLog{EventLog: durable.NewMemoryEventLog()}
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))

	id := durable.SessionID("sess-retry")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fallback.retry) == 0 || fallback.retry[0].Content != "retry" {
		t.Fatalf("want retry event, got %+v", fallback.retry)
	}
	got := drainLog(t, fallback, id)
	var sawMsg bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "after-retry") {
			sawMsg = true
		}
	}
	if !sawMsg {
		t.Fatalf("want after-retry, got %+v", got)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("Status after retry: %+v", st)
	}
}

func TestSessionWorkflow_authExpiredYieldThenResume(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	var calls atomic.Int32
	cloud := tacklr.NewTool(tacklr.ToolConfig{
		Name: "cloud_read",
		Handler: func(context.Context) (string, error) {
			if calls.Add(1) == 1 {
				return "", fmt.Errorf("gdrive: %w", vfs.ErrAuthExpired)
			}
			return "from-cloud", nil
		},
	})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "c1", CallID: "c1", Name: "cloud_read", Arguments: `{}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{cloud}}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))

	id := durable.SessionID("sess-auth")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "read"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalResume, durable.ResumeIn{Responses: map[string][]byte{"c1": []byte(`{}`)}})
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded, saw bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "from-cloud") {
			saw = true
		}
	}
	if !yielded || !saw {
		t.Fatalf("want yield + retried read, got %+v", got)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("Status after auth resume: %+v", st)
	}
}

func TestSessionWorkflow_parallelBatchHitlRunsRemainder(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	var (
		invokes int
		results []string
	)
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		invokes++
		if invokes == 1 {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{
					{ID: "fc_alpha", CallID: "call_alpha", Name: "alpha", Arguments: `{}`},
					{ID: "fc_gate", CallID: "call_gate", Name: "gate", Arguments: `{}`},
					{ID: "fc_beta", CallID: "call_beta", Name: "beta", Arguments: `{}`},
				},
				IsComplete: true,
			}
			return
		}
		for _, m := range msgs {
			if m != nil && m.Role == tacklr.RoleTool {
				results = append(results, m.Content)
			}
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "all-three", IsComplete: true}
	})
	agent := tacklr.AgentOptions{MaxWindowSize: 8192,
		Model: model,
		Tools: []*tacklr.Tool{
			tacklr.NewTool(tacklr.ToolConfig{Name: "alpha", Handler: func(context.Context) (string, error) { return "from-alpha", nil }}),
			tacklr.NewTool(tacklr.ToolConfig{
				Name:    "gate",
				OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
				Handler: func(context.Context) (string, error) { return "gate-ok", nil },
			}),
			tacklr.NewTool(tacklr.ToolConfig{Name: "beta", Handler: func(context.Context) (string, error) { return "from-beta", nil }}),
		}}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))

	id := durable.SessionID("sess-parallel-hitl")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "batch"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, _ := json.Marshal(map[string]string{"optionId": "allow-once"})
		env.SignalWorkflow(signalResume, durable.ResumeIn{Responses: map[string][]byte{"fc_gate": payload}})
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	saw := map[string]bool{}
	for _, c := range results {
		saw[c] = true
	}
	if !saw["from-alpha"] || !saw["gate-ok"] || !saw["from-beta"] {
		t.Fatalf("next model turn missing leftover tool results: %v", results)
	}
}

func TestSessionWorkflow_hitlCancel(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
				Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))

	id := durable.SessionID("sess-hitl-cancel")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "ask"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalCancel, nil)
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want yield before cancel, got %+v", got)
	}
}

func TestSessionWorkflow_mixedBatchPairsBeforeNextRound(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "block-task" {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "block-result", IsComplete: true}
			return
		}
		var sawBlock, sawList bool
		for _, m := range msgs {
			if m == nil || m.Role != tacklr.RoleTool {
				continue
			}
			if m.Content == "block-result" {
				sawBlock = true
			}
			if strings.Contains(m.Content, "Jobs:") || m.Content == "No jobs." {
				sawList = true
			}
		}
		if sawBlock && sawList {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "second-round", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{
				{ID: "b1", CallID: "b1", Name: "spawn_specialist", Arguments: `{"specialist":"blocker","task_description_and_context":"block-task","block":true}`},
				{ID: "l1", CallID: "l1", Name: "list_children", Arguments: `{}`},
			},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{
			{Name: "blocker", Model: model},
		}}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	id := durable.SessionID("sess-mixed-spawn")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 120*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawBlock, sawList, sawSecond bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventToolResult && ev.Content == "block-result" {
			sawBlock = true
		}
		if ev.Type == tacklr.StreamEventToolResult && (strings.Contains(ev.Content, "Jobs:") || ev.Content == "No jobs.") {
			sawList = true
		}
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "second-round" {
			sawSecond = true
		}
	}
	if !sawBlock || !sawList || !sawSecond {
		t.Fatalf("pairing block=%v list=%v second=%v events=%+v", sawBlock, sawList, sawSecond, got)
	}
}

func TestSessionWorkflow_asyncSpawnDoesNotWaitForChild(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	id := durable.SessionID("sess-async-spawn")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "async-child", IsComplete: true}
			return
		}
		var scheduled, collected bool
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleTool && strings.Contains(m.Content, "scheduled") {
				scheduled = true
			}
			if m.Role == tacklr.RoleUser && strings.Contains(m.Content, "completed:") && strings.Contains(m.Content, "async-child") {
				collected = true
			}
		}
		switch {
		case collected:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-continued", IsComplete: true}
		case scheduled:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
				}},
				IsComplete: true,
			}
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "researcher",
			Model: model,
		}}}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	var childStarted atomic.Bool
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want async child SessionWorkflow started")
	}
	got := drainLog(t, fallback, id)
	var sawParent, sawScheduled bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "parent-continued") {
			sawParent = true
		}
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "scheduled") {
			sawScheduled = true
		}
	}
	childGot := drainLog(t, fallback, durable.ChildSessionID(id, "researcher", "sp1"))
	var sawChild bool
	for _, ev := range childGot {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "async-child") {
			sawChild = true
		}
	}
	if !sawScheduled || !sawParent || !sawChild {
		t.Fatalf("scheduled=%v parent=%v child=%v parentEv=%+v childEv=%+v", sawScheduled, sawParent, sawChild, got, childGot)
	}
}

func TestSessionWorkflow_listChildren(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			<-ctx.Done()
			return
		}
		if last != nil && last.Role == tacklr.RoleTool {
			if strings.Contains(last.Content, "cancelled and removed") {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "listed", IsComplete: true}
				return
			}
			if strings.Contains(last.Content, "Jobs:") {
				childID := string(durable.ChildSessionID("sess-list-children", "researcher", "sp1"))
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{{
						ID: "cc1", CallID: "cc1", Name: "cancel_child",
						Arguments: `{"child_id":"` + childID + `"}`,
					}},
					IsComplete: true,
				}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ls1", CallID: "ls1", Name: "list_children", Arguments: `{}`,
				}},
				IsComplete: true,
			}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
				Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name: "researcher", Model: model,
		}}}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	id := durable.SessionID("sess-list-children")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 120*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var listed, cancelled bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "listed" {
			listed = true
		}
		if ev.Type == tacklr.StreamEventToolResult && strings.Contains(ev.Content, "cancelled and removed") {
			cancelled = true
		}
	}
	if !listed || !cancelled {
		t.Fatalf("want listed after cancel_child, listed=%v cancelled=%v got %+v", listed, cancelled, drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_cancelStopsAsyncChild(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "should-not-finish", IsComplete: true}
			}
			return
		}
		for _, m := range msgs {
			if m == nil {
				continue
			}
			for _, tc := range m.ToolCalls {
				if tc.Name == "spawn_specialist" {
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-continued", IsComplete: true}
					return
				}
			}
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
				Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "researcher",
			Model: model,
		}}}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	var childStarted atomic.Bool
	var childErr error
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
		env.SignalWorkflow(signalCancel, nil)
	})
	env.SetOnChildWorkflowCompletedListener(func(info *workflow.Info, result converter.EncodedValue, err error) {
		childErr = err
	})
	id := durable.SessionID("sess-cancel-child")
	childID := durable.ChildSessionID(id, "researcher", "sp1")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want async child started before cancel")
	}
	st := querySession(t, env)
	if st.State != durable.SessionFailed || st.Waiting {
		t.Fatalf("parent status %+v", st)
	}
	if childErr == nil {
		if val, qerr := env.QueryWorkflowByID(string(childID), queryStatus); qerr == nil {
			var cst durable.SessionStatus
			if err := val.Get(&cst); err == nil && cst.State == durable.SessionFailed {
				return
			}
		}
		t.Fatalf("want child canceled/failed, parent=%+v", st)
	}
}

func TestSessionWorkflow_steerDuringYieldKeepsPark(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	var n atomic.Int32
	var resumed, invoke2BeforeResume, sawSteer, toolThenSteer atomic.Bool
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		i := n.Add(1)
		if i == 1 {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
					Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
				}},
				IsComplete: true,
			}
			return
		}
		if !resumed.Load() {
			invoke2BeforeResume.Store(true)
		}
		var seq []string
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleTool && m.ToolCallID == "ask1" {
				seq = append(seq, "result")
			}
			if m.Role == tacklr.RoleUser && m.Content == "steer" {
				seq = append(seq, "steer")
				sawSteer.Store(true)
			}
		}
		if i == 2 && strings.Contains(strings.Join(seq, ","), "result,steer") {
			toolThenSteer.Store(true)
		}
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "chose", IsComplete: true}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	id := durable.SessionID("sess-steer-yield")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "ask"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "steer"})
		st := querySession(t, env)
		if !st.Waiting {
			t.Error("steer must keep the park")
		}
		if n.Load() != 1 {
			t.Errorf("Invoke 2 must not run before Resume, got %d", n.Load())
		}
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		resumed.Store(true)
		payload, _ := json.Marshal(map[string]any{"selectionIdx": 0})
		env.SignalWorkflow(signalResume, durable.ResumeIn{Responses: map[string][]byte{"ask1": payload}})
	}, 40*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if invoke2BeforeResume.Load() {
		t.Fatal("park was cleared; Invoke 2 ran before Resume")
	}
	if !sawSteer.Load() || !toolThenSteer.Load() {
		t.Fatal("Invoke 2 must see ask_user tool result then steer")
	}
	got := drainLog(t, fallback, id)
	var yielded bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventInterrupt {
			yielded = true
		}
	}
	if !yielded {
		t.Fatalf("want park kept through steer, got %+v", got)
	}
}

func TestSessionWorkflow_failedAsyncChildJobAbsorbed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	id := durable.SessionID("sess-async-fail")
	childID := durable.ChildSessionID(id, "researcher", "sp1")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Content: "boom-child", IsComplete: true}
			return
		}
		var scheduled, collected bool
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleTool && strings.Contains(m.Content, "scheduled") {
				scheduled = true
			}
			if m.Role == tacklr.RoleUser && strings.Contains(m.Content, "failed:") && strings.Contains(m.Content, string(childID)) {
				collected = true
			}
		}
		switch {
		case collected:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-continued", IsComplete: true}
		case scheduled:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
					Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
				}},
				IsComplete: true,
			}
		}
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "researcher",
			Model: model,
		}}}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := drainLog(t, fallback, id)
	var sawParent bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-continued" {
			sawParent = true
		}
	}
	if !sawParent {
		t.Fatalf("parent must absorb failed job RoleUser, events=%+v", got)
	}
	val, err := env.QueryWorkflow(queryChildren)
	if err != nil {
		t.Fatal(err)
	}
	var kids []durable.SessionID
	if err := val.Get(&kids); err != nil {
		t.Fatal(err)
	}
	for _, k := range kids {
		if k == childID {
			t.Fatalf("failed child still listed: %v", kids)
		}
	}
}

func TestSessionWorkflow_workerChildCompletesParent(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	id := durable.SessionID("sess-worker")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		var scheduled, collected bool
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Role == tacklr.RoleTool && strings.Contains(m.Content, "scheduled") {
				scheduled = true
			}
			if m.Role == tacklr.RoleUser && strings.Contains(m.Content, "completed:") && strings.Contains(m.Content, "green") {
				collected = true
			}
		}
		switch {
		case collected:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
		case scheduled:
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "waiting", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ci1", CallID: "ci1", Name: "watch_ci", Arguments: `{}`,
				}},
				IsComplete: true,
			}
		}
	})
	watch := tacklr.NewTool(tacklr.ToolConfig{
		Name: "watch_ci",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
			job, err := runtime.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "pipe"})
			if err != nil {
				return "", err
			}
			return "Job " + job.ID + " scheduled (name=ci).", nil
		},
	})
	agent := tacklr.AgentOptions{Model: model,
		MaxWindowSize: 8192,
		Tools:         []*tacklr.Tool{watch}}
	fallback := durable.NewMemoryEventLog()
	acts := newActs(agent, fallback, true)
	acts.Jobs = map[string]durable.JobHandler{
		"ci": func(ctx context.Context, task string) (string, error) { return "green", nil },
	}
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(acts)
	var childStarted atomic.Bool
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, ctx workflow.Context, args converter.EncodedValues) {
		childStarted.Store(true)
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !childStarted.Load() {
		t.Fatal("want worker child SessionWorkflow")
	}
	got := drainLog(t, fallback, id)
	var sawDone bool
	for _, ev := range got {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done" {
			sawDone = true
		}
	}
	if !sawDone {
		t.Fatalf("want parent-done after worker job, events=%+v", got)
	}
}

func TestSessionWorkflow_toolFailureStaysInTheWindow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	boom := tacklr.NewTool(tacklr.ToolConfig{
		Name: "boom",
		Handler: func(context.Context) (string, error) {
			return "", fmt.Errorf("upstream: the search provider failed: %w", tacklr.ErrFailed)
		},
	})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "b1", CallID: "b1", Name: "boom", Arguments: `{}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{boom}}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	id := durable.SessionID("sess-tool-failed")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "search provider failed") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want the failure text in the assistant reply, got %+v", drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_missingPathIsACorrection(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	dir := t.TempDir()
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "r1", CallID: "r1", Name: "read",
				Arguments: `{"path":"/workspace/docs/missing.txt"}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		OpenVFS: vfs.Tree(vfs.At("docs", vfs.Local(dir))),
	}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	id := durable.SessionID("sess-missing-path")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "read"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != durable.SessionComplete {
		t.Fatalf("status %+v", st)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "does not exist") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want the missing-path correction in the reply, got %+v", drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_badWorkspaceBindingFailsTurn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	missing := filepath.Join(t.TempDir(), "missing")
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "r1", CallID: "r1", Name: "read",
				Arguments: `{"path":"/workspace/docs/hello.txt"}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		OpenVFS: vfs.Tree(vfs.At("docs", vfs.Local(missing))),
	}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	id := durable.SessionID("sess-bad-binding")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, durable.PromptIn{
			Text: "read",
			Auth: durable.AuthContext{Bindings: []vfs.Binding{{
				Provider: "local",
				Params:   map[string]string{vfs.ParamName: "docs"},
				Auth:     vfs.Credential{Token: "x"},
			}}},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 80*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if st := querySession(t, env); st.State != durable.SessionFailed {
		t.Fatalf("status %+v events %+v", st, drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_grandchildResultReachesParent(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "leaf-task" {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "leaf-answer", IsComplete: true}
			return
		}
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "go-deeper" {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "leaf1", CallID: "leaf1", Name: "spawn_specialist",
					Arguments: `{"specialist":"leaf","task_description_and_context":"leaf-task","block":true}`,
				}},
				IsComplete: true,
			}
			return
		}
		if last != nil && last.Role == tacklr.RoleTool && strings.Contains(last.Content, "leaf-answer") {
			var parent bool
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleUser && m.Content == "go" {
					parent = true
				}
			}
			if parent {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "got-grandchild", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "leaf-answer", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "mid1", CallID: "mid1", Name: "spawn_specialist",
				Arguments: `{"specialist":"mid","task_description_and_context":"go-deeper","block":true}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{
			Name:  "mid",
			Model: model,
			Specialists: []*tacklr.Specialist{{
				Name: "leaf", Model: model,
			}},
		}},
	}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	id := durable.SessionID("sess-grandchild")
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 150*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "got-grandchild" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want the grandchild text to finish the parent, got %+v", drainLog(t, fallback, id))
	}
}

func TestSessionWorkflow_parkedParentLeavesAsyncChildRunning(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	release := make(chan struct{})
	var once sync.Once
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := lastMsg(msgs)
		if last != nil && last.Role == tacklr.RoleUser && last.Content == "child-task" {
			select {
			case <-ctx.Done():
				return
			case <-release:
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "child-alive", IsComplete: true}
			return
		}
		for _, m := range msgs {
			if m != nil && m.Role == tacklr.RoleTool && strings.Contains(m.Content, "User selected") {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-resumed", IsComplete: true}
				return
			}
		}
		if last != nil && last.Role == tacklr.RoleTool && strings.Contains(last.Content, "scheduled") {
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
					Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
				}},
				IsComplete: true,
			}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventFunctionCall,
			ToolCalls: []tacklr.ToolCall{{
				ID: "sp1", CallID: "sp1", Name: "spawn_specialist",
				Arguments: `{"specialist":"researcher","task_description_and_context":"child-task","block":false}`,
			}},
			IsComplete: true,
		}
	})
	agent := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192,
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}},
	}
	fallback := durable.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(agent, fallback, true))
	id := durable.SessionID("sess-park-child")
	childID := durable.ChildSessionID(id, "researcher", "sp1")
	var sawChild bool
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalPrompt, durable.PromptIn{Text: "go"}) }, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(queryChildren)
		if err != nil {
			t.Fatal(err)
		}
		var ids []durable.SessionID
		if err := val.Get(&ids); err != nil {
			t.Fatal(err)
		}
		for _, got := range ids {
			if got == childID {
				sawChild = true
			}
		}
		if st := querySession(t, env); !st.Waiting {
			t.Fatalf("parent should be parked, status %+v", st)
		}
		once.Do(func() { close(release) })
	}, 40*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalResume, durable.ResumeIn{
			Responses: map[string][]byte{"ask1": []byte(`{"selectionIdx":0}`)},
		})
	}, 60*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalClose, nil) }, 100*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, workflowInput{SessionID: id})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !sawChild {
		t.Fatal("async child was gone while the parent was parked")
	}
	var resumed bool
	for _, ev := range drainLog(t, fallback, id) {
		if ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-resumed" {
			resumed = true
		}
	}
	if !resumed {
		t.Fatalf("want the parent to resume, got %+v", drainLog(t, fallback, id))
	}
}
