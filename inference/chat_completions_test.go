package inference

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pkoukk/tiktoken-go"

	"github.com/ryanaldo34/tacklr"
)

func TestChatCompletions_streamsTextReasoningAndToolCall(t *testing.T) {
	var sawBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		if err := json.Unmarshal(raw, &sawBody); err != nil {
			t.Errorf("unmarshal body: %v", err)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"id":"1","choices":[{"index":0,"delta":{"role":"assistant","reasoning":"thinking hard"},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_9","type":"function","function":{"name":"echo","arguments":"{\"a\":"}}]},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"completion_tokens_details":{"reasoning_tokens":2}}}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("test-key").
		WithModel("stealth/ox-alpha").
		WithURL(srv.URL).
		WithReasoningLevel("high")
	s.SetSystemPrompt("sys instructions")

	msgs := []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "hi"},
		{Role: tacklr.RoleReasoning, Content: "dropped thought"},
		{Role: tacklr.RoleAssistant, Content: "", ToolCalls: []tacklr.ToolCall{{CallID: "call_1", Name: "echo", Arguments: "{}"}}},
		{Role: tacklr.RoleTool, ToolCallID: "call_1", Content: "ok"},
	}
	tools := []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "echo", Handler: func(ctx context.Context) (string, error) { return "ok", nil }}),
	}

	ch, err := s.Invoke(context.Background(), msgs, tools, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	var text, reasoning string
	var fc *tacklr.LLMResponseChunk
	var complete *tacklr.LLMResponseChunk
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			t.Fatalf("stream error: %s", chunk.Content)
		}
		switch chunk.Type {
		case tacklr.StreamEventMessage:
			text += chunk.Content
		case tacklr.StreamEventReasoning:
			reasoning += chunk.Content
		case tacklr.StreamEventFunctionCall:
			c := chunk
			fc = &c
		case tacklr.StreamEventComplete:
			c := chunk
			complete = &c
		}
	}

	if text != "hello" {
		t.Errorf("text = %q, want hello", text)
	}
	if reasoning != "thinking hard" {
		t.Errorf("reasoning = %q, want thinking hard", reasoning)
	}
	if fc == nil || len(fc.ToolCalls) != 1 {
		t.Fatalf("function_call chunk = %#v", fc)
	}
	tc := fc.ToolCalls[0]
	if tc.CallID != "call_9" || tc.Name != "echo" || tc.Arguments != `{"a":1}` || !fc.IsComplete {
		t.Errorf("tool call = %#v", tc)
	}
	if complete == nil || complete.InputTokens != 10 || complete.OutputTokens != 5 || complete.ReasoningTokens != 2 {
		t.Errorf("complete = %#v", complete)
	}

	if sawBody == nil {
		t.Fatal("request body not captured")
	}
	if sawBody["model"] != "stealth/ox-alpha" {
		t.Errorf("model = %v", sawBody["model"])
	}
	if sawBody["stream"] != true {
		t.Errorf("stream = %v, want true", sawBody["stream"])
	}
	so, _ := sawBody["stream_options"].(map[string]any)
	if so["include_usage"] != true {
		t.Errorf("stream_options = %v", sawBody["stream_options"])
	}
	rs, _ := sawBody["reasoning"].(map[string]any)
	if rs["effort"] != "high" {
		t.Errorf("reasoning = %v", sawBody["reasoning"])
	}
	if sawBody["include_reasoning"] != true {
		t.Errorf("include_reasoning = %v", sawBody["include_reasoning"])
	}

	msgRaw, _ := json.Marshal(sawBody["messages"])
	var messages []map[string]any
	if err := json.Unmarshal(msgRaw, &messages); err != nil {
		t.Fatalf("messages: %v body=%s", err, msgRaw)
	}
	var roles []string
	for _, m := range messages {
		roles = append(roles, m["role"].(string))
	}
	if len(roles) != 4 || roles[0] != "system" || roles[1] != "user" || roles[2] != "assistant" || roles[3] != "tool" {
		t.Fatalf("message roles = %v, want [system user assistant tool]", roles)
	}
	asst := messages[2]
	calls, _ := asst["tool_calls"].([]any)
	if len(calls) != 1 {
		t.Fatalf("assistant tool_calls = %v", asst["tool_calls"])
	}
	call := calls[0].(map[string]any)
	if call["id"] != "call_1" {
		t.Errorf("assistant tool call id = %v", call["id"])
	}
	if messages[3]["tool_call_id"] != "call_1" {
		t.Errorf("tool message tool_call_id = %v", messages[3]["tool_call_id"])
	}

	toolsRaw, _ := json.Marshal(sawBody["tools"])
	var toolsList []map[string]any
	if err := json.Unmarshal(toolsRaw, &toolsList); err != nil {
		t.Fatalf("tools unmarshal: %v body=%s", err, toolsRaw)
	}
	if len(toolsList) != 1 || toolsList[0]["type"] != "function" {
		t.Fatalf("tools = %s", toolsRaw)
	}
	fn, _ := toolsList[0]["function"].(map[string]any)
	if fn["name"] != "echo" || fn["parameters"] == nil {
		t.Errorf("function wrapper = %s", toolsRaw)
	}
}

func TestChatCompletions_finishReasonLength_mapsToMaxTokens(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"id":"1","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var streamErr error
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			streamErr = chunk.Error
		}
	}
	if !errors.Is(streamErr, tacklr.ErrMaxTokens) {
		t.Fatalf("stream error = %v, want ErrMaxTokens", streamErr)
	}
}

func TestChatCompletions_finishReasonContentFilter_mapsToRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var streamErr error
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			streamErr = chunk.Error
		}
	}
	if !errors.Is(streamErr, tacklr.ErrModelRefused) {
		t.Fatalf("stream error = %v, want ErrModelRefused", streamErr)
	}
}

func TestChatCompletions_doneWithoutFinish_incomplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"1","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`+"\n"+
			`data: [DONE]`+"\n")
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var streamErr error
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			streamErr = chunk.Error
		}
	}
	if !errors.Is(streamErr, ErrIncompleteStream) {
		t.Fatalf("stream error = %v, want ErrIncompleteStream", streamErr)
	}
}

func TestChatCompletions_httpError_emitsAPIStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"message":"reasoning mandatory","type":"invalid_request_error"}}`)
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke sync err: %v", err)
	}
	var saw string
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			saw = chunk.Content
			var apiErr *APIStatusError
			if !errors.As(chunk.Error, &apiErr) || apiErr.Status != http.StatusBadRequest {
				t.Errorf("error not APIStatusError(400): %v", chunk.Error)
			}
		}
	}
	if !strings.Contains(saw, "reasoning mandatory") {
		t.Errorf("error content = %q, want reasoning mandatory", saw)
	}
}

func TestChatCompletions_streamErrorEvent_emitsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"error":{"message":"mid-stream boom","type":"server_error"}}`+"\n")
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var saw string
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			saw = chunk.Content
		}
	}
	if !strings.Contains(saw, "mid-stream boom") {
		t.Errorf("error content = %q, want mid-stream boom", saw)
	}
}

func TestChatCompletions_usageOnlyChunkAfterTerminal_emitsComplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"id":"1","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: {"id":"1","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4}}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	seen := 0
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			t.Fatalf("stream error: %s", chunk.Content)
		}
		if chunk.Type == tacklr.StreamEventComplete {
			seen++
			if chunk.InputTokens != 3 || chunk.OutputTokens != 4 {
				t.Errorf("complete = %#v", chunk)
			}
		}
	}
	if seen != 1 {
		t.Errorf("complete chunks = %d, want 1 (usage-only chunk after finish)", seen)
	}
}

func TestChatCompletions_contextCancel_stopsStream(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 1000; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			_, _ = io.WriteString(w, `data: {"id":"1","choices":[{"index":0,"delta":{"content":"x"},"finish_reason":null}]}`+"\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.Invoke(ctx, []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not start")
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("no chunks")
	}
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("stream did not end after cancel")
		}
	}
}

func TestChatCompletions_CountTokens_localFallback(t *testing.T) {
	s := NewOpenRouterInferenceStrategy(nil).
		WithApiKey("k").
		WithModel("m")

	n, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "count these tokens please"},
	}, nil)
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected positive count, got %d", n)
	}
}

func TestChatCompletions_buildersAndCapabilities(t *testing.T) {
	s := NewOpenRouterInferenceStrategy(nil)
	if s.maxContextWindow != defaultOpenRouterContextWindow {
		t.Errorf("default window = %d", s.maxContextWindow)
	}
	if s.baseURL != "https://openrouter.ai/api/v1" {
		t.Errorf("default baseURL = %q", s.baseURL)
	}
	s.WithMaxContextWindow(0).WithMaxOutputTokens(-5)
	if s.maxContextWindow != defaultOpenRouterContextWindow {
		t.Errorf("WithMaxContextWindow(0) should keep default, got %d", s.maxContextWindow)
	}
	if s.maxOutputTokens != 0 {
		t.Errorf("WithMaxOutputTokens(-5) should clamp to 0, got %d", s.maxOutputTokens)
	}
	s.WithMaxContextWindow(4096).WithMaxOutputTokens(2048).WithReasoningLevel("max").WithURL("https://example.test/v1").WithModel("m")
	if s.maxContextWindow != 4096 || s.maxOutputTokens != 2048 || s.reasoning != "max" || s.baseURL != "https://example.test/v1" {
		t.Errorf("builder state = %#v", s)
	}
	w, err := s.MaxContextWindow()
	if err != nil || w != 4096 {
		t.Errorf("MaxContextWindow = %d, %v", w, err)
	}
	if !s.SupportsMIME("text/plain") || !s.SupportsMIME("") {
		t.Errorf("text mimes rejected")
	}
	if s.SupportsMIME("image/png") || s.SupportsMIME("application/pdf") {
		t.Errorf("non-text mimes accepted")
	}
	var nilS *OpenRouterInferenceStrategy
	if id := nilS.ModelTelemetryIdentity(); id.Model != "" {
		t.Errorf("nil identity = %#v", id)
	}
	if id := s.ModelTelemetryIdentity(); id.Model != "m" {
		t.Errorf("identity model = %q", id.Model)
	}
}

func TestChatCompletions_validation(t *testing.T) {
	s := NewOpenRouterInferenceStrategy(nil)
	if _, err := s.Invoke(context.Background(), nil, nil, ""); !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Errorf("Invoke no key = %v", err)
	}
	if _, err := s.CountTokens(context.Background(), nil, nil); !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Errorf("CountTokens no key = %v", err)
	}
	s.WithApiKey("k")
	if _, err := s.Invoke(context.Background(), nil, nil, ""); !errors.Is(err, tacklr.ErrModelNotSet) {
		t.Errorf("Invoke no model = %v", err)
	}
	if _, err := s.CountTokens(context.Background(), nil, nil); !errors.Is(err, tacklr.ErrModelNotSet) {
		t.Errorf("CountTokens no model = %v", err)
	}
}

func TestChatCompletions_CountTokens_branchesAndEncodingError(t *testing.T) {
	prev := getEncoding
	t.Cleanup(func() { getEncoding = prev })
	getEncoding = func(string) (*tiktoken.Tiktoken, error) {
		return nil, errors.New("no encoding")
	}
	s := NewOpenRouterInferenceStrategy(nil).WithApiKey("k").WithModel("m")
	if _, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "x"}}, nil); err == nil || !strings.Contains(err.Error(), "tiktoken") {
		t.Fatalf("err = %v", err)
	}

	getEncoding = prev
	tool := tacklr.NewTool(tacklr.ToolConfig{Name: "echo", Handler: func(ctx context.Context) (string, error) { return "ok", nil }})
	msgs := []*tacklr.Message{
		nil,
		{Role: tacklr.RoleTool, ToolCallID: "c1", Content: "result"},
		{Role: tacklr.RoleAssistant, Content: "a", ToolCalls: []tacklr.ToolCall{{Name: "echo", Arguments: `{"x":1}`}}},
	}
	n, err := s.CountTokens(context.Background(), msgs, []*tacklr.Tool{tool})
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected positive count, got %d", n)
	}
}

func TestChatCompletions_Invoke_maxTokensParam(t *testing.T) {
	var sawBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &sawBody)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"1","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}`+"\n"+
			`data: {"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`+"\n"+
			`data: [DONE]`+"\n")
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL).
		WithMaxOutputTokens(4096)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			t.Fatalf("stream error: %s", chunk.Content)
		}
	}
	if sawBody["max_tokens"] != float64(4096) {
		t.Errorf("max_tokens = %v", sawBody["max_tokens"])
	}
}

func TestChatCompletions_malformedStream_incomplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: not-json\n")
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var streamErr error
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			streamErr = chunk.Error
		}
	}
	if !errors.Is(streamErr, ErrMalformedStream) {
		t.Fatalf("stream error = %v, want ErrMalformedStream", streamErr)
	}
}

func TestChatCompletions_streamClosedBeforeFinish_incomplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"1","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`+"\n")
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var streamErr error
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			streamErr = chunk.Error
		}
	}
	if !errors.Is(streamErr, ErrIncompleteStream) {
		t.Fatalf("stream error = %v, want ErrIncompleteStream", streamErr)
	}
}

func TestChatCompletions_emptyArgsAndToolCallMergeOverrides(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"id":"1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_x","type":"function","function":{"name":"echo"}}]},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_y","function":{"name":"other"}}]},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_z","type":"function","function":{"name":"second","arguments":"{}"}}]},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var fc *tacklr.LLMResponseChunk
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			t.Fatalf("stream error: %s", chunk.Content)
		}
		if chunk.Type == tacklr.StreamEventFunctionCall {
			c := chunk
			fc = &c
		}
	}
	if fc == nil || len(fc.ToolCalls) != 2 {
		t.Fatalf("function_call chunk = %#v, want 2 tool calls", fc)
	}
	tc := fc.ToolCalls[0]
	if tc.CallID != "call_y" || tc.Name != "other" || tc.Arguments != "{}" {
		t.Errorf("tool call 0 = %#v, want merged id/name and empty-object args", tc)
	}
	tc2 := fc.ToolCalls[1]
	if tc2.CallID != "call_z" || tc2.Name != "second" || tc2.Arguments != "{}" {
		t.Errorf("tool call 1 = %#v", tc2)
	}
}

type chatErrTransport struct{}

func (chatErrTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("connection refused")
}

func TestChatCompletions_transportError_emitsError(t *testing.T) {
	client := &http.Client{Transport: chatErrTransport{}}
	s := NewOpenRouterInferenceStrategy(client).
		WithApiKey("k").
		WithModel("m").
		WithURL("https://example.test/v1")

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke sync err: %v", err)
	}
	var saw string
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			saw = chunk.Content
		}
	}
	if !strings.Contains(saw, "connection refused") {
		t.Errorf("error content = %q, want connection refused", saw)
	}
}

func TestChatCompletions_reasoningDetailsVariants(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"id":"1","choices":[{"index":0,"delta":{"reasoning_details":"first"},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{"reasoning_details":[{"text":"second"}]},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{"reasoning_details":{"x":1}},"finish_reason":null}]}`,
			`data: {"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var reasoning string
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			t.Fatalf("stream error: %s", chunk.Content)
		}
		if chunk.Type == tacklr.StreamEventReasoning {
			reasoning += chunk.Content
		}
	}
	if reasoning != "firstsecond" {
		t.Errorf("reasoning = %q, want firstsecond", reasoning)
	}
}

func TestChatCompletions_marshalMessages_dropsAndPairs(t *testing.T) {
	msgs := []*tacklr.Message{
		{Role: tacklr.RoleDeveloper, Content: "dev"},
		{Role: tacklr.RoleReasoning, Content: "think"},
		{Role: tacklr.RoleTool, ToolCallID: "", Content: "no-id"},
		{Role: tacklr.RoleTool, ToolCallID: "ghost", Content: "unmatched"},
		{Role: tacklr.RoleAssistant, Content: "", ToolCalls: nil},
		{Role: tacklr.RoleAssistant, Content: "hi", ToolCalls: []tacklr.ToolCall{
			{CallID: "c1", Name: "echo", Arguments: ""},
			{CallID: "c2", Name: "read", Namespace: "vfs", Arguments: "{}"},
			{CallID: "", Name: "bad"},
		}},
		{Role: tacklr.RoleTool, ToolCallID: "c1", Content: "ok"},
		{Role: tacklr.RoleTool, ToolCallID: "ghost", Content: "still unmatched"},
		nil,
	}

	out := marshalMessagesToChat(msgs)
	if len(out) != 3 {
		t.Fatalf("messages = %#v, want [system assistant tool]", out)
	}
	if out[0].Role != "system" || out[0].Content != "dev" {
		t.Errorf("first = %#v, want system/dev", out[0])
	}
	if out[1].Role != "assistant" || out[1].Content != "hi" || len(out[1].ToolCalls) != 2 {
		t.Errorf("assistant = %#v", out[1])
	}
	tc1, tc2 := out[1].ToolCalls[0], out[1].ToolCalls[1]
	if tc1.ID != "c1" || tc1.Function.Name != "echo" || tc1.Function.Arguments != "{}" {
		t.Errorf("tc1 = %#v", tc1)
	}
	if tc2.ID != "c2" || tc2.Function.Name != "vfs.read" || tc2.Function.Arguments != "{}" {
		t.Errorf("tc2 = %#v", tc2)
	}
	if out[2].Role != "tool" || out[2].ToolCallID != "c1" || out[2].Content != "ok" {
		t.Errorf("tool = %#v", out[2])
	}
}

func TestChatCompletions_summarize_emptyAndTruncated(t *testing.T) {
	if s := summarizeChatMessages(nil); s != "empty" {
		t.Errorf("empty summary = %q", s)
	}
	msgs := make([]chatMessage, 30)
	s := summarizeChatMessages(msgs)
	if !strings.Contains(s, "…+5 more") {
		t.Errorf("truncated summary = %q", s)
	}
	tc := chatMessage{Role: "assistant", ToolCalls: []chatToolCall{{ID: "c"}}}
	if s := summarizeChatMessages([]chatMessage{{Role: "user"}, tc}); !strings.Contains(s, "tool_calls=1") {
		t.Errorf("tool_calls summary = %q", s)
	}
}
