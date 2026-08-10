package tacklr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func testHarnessRuntime() HarnessRuntime {
	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
	})
	return turnRuntime(ah)
}

func TestDownloadGate_skipWhenDisabled(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "shell",
		Handler: func(ctx context.Context) (string, error) { return "ok", nil },
	})
	next := func(ctx context.Context, inv ToolInvocation) (string, error) {
		return "ok", nil
	}
	inv := ToolInvocation{Tool: tool, ArgsJSON: `{"cmd":"npm install foo"}`}

	gate := downloadGate(nil)
	out, err := gate(context.Background(), inv, next)
	if err != nil || out != "ok" {
		t.Fatalf("nil cfg: out=%q err=%v", out, err)
	}

	gate = downloadGate(&DownloadGateConfig{Enabled: false})
	out, err = gate(context.Background(), inv, next)
	if err != nil || out != "ok" {
		t.Fatalf("disabled: out=%q err=%v", out, err)
	}
}

func TestDownloadGate_skipWhenApprovalReasonSet(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:           "shell",
		ApprovalReason: "shell",
		Handler:        func(ctx context.Context) (string, error) { return "ok", nil },
	})
	next := func(ctx context.Context, inv ToolInvocation) (string, error) {
		return "ok", nil
	}
	gate := downloadGate(&DownloadGateConfig{Enabled: true})
	out, err := gate(context.Background(), ToolInvocation{Tool: tool, ArgsJSON: `{"cmd":"npm install foo"}`}, next)
	if err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestDownloadGate_skipWhenExemptTool(t *testing.T) {
	tool := NewTool(ToolConfig{
		Name:    "shell",
		Handler: func(ctx context.Context) (string, error) { return "ok", nil },
	})
	next := func(ctx context.Context, inv ToolInvocation) (string, error) {
		return "ok", nil
	}
	gate := downloadGate(&DownloadGateConfig{Enabled: true, ExemptTools: []string{"shell"}})
	out, err := gate(context.Background(), ToolInvocation{Tool: tool, ArgsJSON: `{"cmd":"npm install foo"}`}, next)
	if err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestDownloadGate_nilTool(t *testing.T) {
	next := func(ctx context.Context, inv ToolInvocation) (string, error) {
		return "ok", nil
	}
	gate := downloadGate(&DownloadGateConfig{Enabled: true})
	out, err := gate(context.Background(), ToolInvocation{}, next)
	if err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestDownloadGate_skipWhenNoMatch(t *testing.T) {
	rt := testHarnessRuntime()
	gate := downloadGate(&DownloadGateConfig{Enabled: true})
	_, err := gate(context.Background(), ToolInvocation{
		Tool: NewTool(ToolConfig{
			Name:    "shell",
			Handler: func(ctx context.Context) (string, error) { return "ok", nil },
		}),
		ArgsJSON: `{"cmd":"echo hello world"}`,
		Runtime:  rt,
	}, streamEof)
	if err != nil {
		t.Fatalf("no match should pass through: err=%v", err)
	}
}

func TestDownloadGate_noFalsePositiveOnHarmlessStrings(t *testing.T) {
	tests := []struct {
		name string
		args string
	}{
		{"echo", `{"cmd":"echo hello world"}`},
		{"ls", `{"cmd":"ls -la"}`},
		{"cat file", `{"cmd":"cat /etc/hosts"}`},
		{"node version", `{"cmd":"node --version"}`},
		{"npm version", `{"cmd":"npm --version"}`},
		{"python script", `{"cmd":"python main.py"}`},
	}
	rt := testHarnessRuntime()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := downloadGate(&DownloadGateConfig{Enabled: true})
			_, err := gate(context.Background(), ToolInvocation{
				Tool: NewTool(ToolConfig{
					Name:    "shell",
					Handler: func(ctx context.Context) (string, error) { return "ok", nil },
				}),
				ArgsJSON: tt.args,
				Runtime:  rt,
			}, streamEof)
			if err != nil {
				t.Fatalf("err=%v: false positive for %q", err, tt.args)
			}
		})
	}
}

func TestDownloadGate_plaintextArgsJSON(t *testing.T) {
	rt := testHarnessRuntime()
	gate := downloadGate(&DownloadGateConfig{Enabled: true})
	for _, args := range []string{`""`, `[]`, `null`, `{"key": 42}`} {
		_, err := gate(context.Background(), ToolInvocation{
			Tool: NewTool(ToolConfig{
				Name:    "shell",
				Handler: func(ctx context.Context) (string, error) { return "ok", nil },
			}),
			ArgsJSON: args,
			Runtime:  rt,
		}, streamEof)
		if err != nil {
			t.Fatalf("args=%q err=%v", args, err)
		}
	}
}

func TestDownloadGate_patternDetection(t *testing.T) {
	tests := []string{
		`{"cmd":"npm install lodash"}`,
		`{"cmd":"npm i express"}`,
		`{"cmd":"pip install requests"}`,
		`{"cmd":"pip3 install numpy"}`,
		`{"cmd":"yarn add react"}`,
		`{"cmd":"pnpm add typescript"}`,
		`{"cmd":"apt-get install curl"}`,
		`{"cmd":"brew install node"}`,
		`{"cmd":"cargo install ripgrep"}`,
		`{"cmd":"go install github.com/foo/bar@latest"}`,
		`{"cmd":"wget https://example.com/file.tar.gz"}`,
		`{"cmd":"curl -O https://example.com/file.tar.gz"}`,
		`{"cmd":"git clone https://github.com/foo/bar.git"}`,
		`{"cmd":"playwright install chromium"}`,
		`{"cmd":"npx playwright install"}`,
		`{"target":"https://download.example.com/binary"}`,
		`{"cmd":"bun add hono"}`,
		`{"cmd":"cargo add serde"}`,
		`{"cmd":"yarn install"}`,
	}
	rt := testHarnessRuntime()
	for _, args := range tests {
		t.Run(args[:min(len(args), 60)], func(t *testing.T) {
			gate := downloadGate(&DownloadGateConfig{Enabled: true})
			_, err := gate(context.Background(), ToolInvocation{
				Tool: NewTool(ToolConfig{
					Name:    "shell",
					Handler: func(ctx context.Context) (string, error) { return "ok", nil },
				}),
				ArgsJSON: args,
				Runtime:  rt,
			}, streamEof)
			if err == nil {
				t.Fatalf("expected download detection: %s", args)
			}
		})
	}
}

func TestDownloadGate_matchNestedArgs(t *testing.T) {
	rt := testHarnessRuntime()
	gate := downloadGate(&DownloadGateConfig{Enabled: true})
	_, err := gate(context.Background(), ToolInvocation{
		Tool: NewTool(ToolConfig{
			Name:    "exec",
			Handler: func(ctx context.Context) (string, error) { return "ok", nil },
		}),
		ArgsJSON: `{"steps":[{"run":"node --version"},{"run":"pip install requests"}]}`,
		Runtime:  rt,
	}, streamEof)
	if err == nil {
		t.Fatal("expected error from nested download detection")
	}
}

func TestDownloadGate_customPatterns(t *testing.T) {
	rt := testHarnessRuntime()
	tool := &ToolInvocation{
		Tool: NewTool(ToolConfig{
			Name:    "shell",
			Handler: func(ctx context.Context) (string, error) { return "ok", nil },
		}),
		Runtime: rt,
	}

	defaultGate := downloadGate(&DownloadGateConfig{Enabled: true})
	_, err := defaultGate(context.Background(), ToolInvocation{Tool: tool.Tool, ArgsJSON: `{"cmd":"choco install firefox"}`, Runtime: rt}, streamEof)
	if err != nil {
		t.Fatalf("default patterns should not catch choco: err=%v", err)
	}

	customGate := downloadGate(&DownloadGateConfig{
		Enabled:  true,
		Patterns: []string{`choco\s+install`},
	})
	_, err = customGate(context.Background(), ToolInvocation{Tool: tool.Tool, ArgsJSON: `{"cmd":"choco install firefox"}`, Runtime: rt}, streamEof)
	if err == nil {
		t.Fatal("custom patterns should catch choco")
	}
}

func TestHarness_downloadGate_detectAndReject(t *testing.T) {
	shellTool := NewTool(ToolConfig{
		Name:    "shell",
		Handler: func(ctx context.Context) (string, error) { return "executed", nil },
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "s1", CallID: "s1", Name: "shell", Arguments: `{"cmd":"npm install foo"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	ah := NewAgent(context.Background(), AgentOptions{
		Config:               Config{MaxWindowSize: 8192},
		Model:                strategy,
		Tools:                []*Tool{shellTool},
		DownloadInterception: &DownloadGateConfig{Enabled: true},
	})

	ch1, err := ah.Run(context.Background(), "install something")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	var interruptType string
	for ev := range ch1 {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
				Type        string `json:"type"`
				Data        struct {
					ToolName string `json:"toolName"`
					Reason   string `json:"reason"`
				} `json:"data"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err != nil {
				t.Fatal(err)
			}
			interruptID = payload.InterruptId
			interruptType = payload.Type
			if payload.Data.ToolName != "shell" {
				t.Fatalf("toolName = %q", payload.Data.ToolName)
			}
			if payload.Data.Reason != "download_detected" {
				t.Fatalf("reason = %q", payload.Data.Reason)
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected download interrupt")
	}
	if interruptType != "tool_permission" {
		t.Fatalf("type = %q, want tool_permission", interruptType)
	}

	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"reject-once"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch2 {
	}

	var denied bool
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleTool && strings.Contains(m.Content, "permission denied") {
			denied = true
		}
	}
	if !denied {
		t.Fatal("expected permission denied after reject")
	}
}

func TestHarness_downloadGate_allowAlwaysRemembers(t *testing.T) {
	var handlerCalls int
	shellTool := NewTool(ToolConfig{
		Name: "shell",
		Handler: func(ctx context.Context) (string, error) {
			handlerCalls++
			return "executed", nil
		},
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			switch {
			case invokeCount == 1 || invokeCount == 3:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "s1", CallID: "s1", Name: "shell", Arguments: `{"cmd":"npm install foo"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			default:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			}
		},
	}
	ah := NewAgent(context.Background(), AgentOptions{
		Config:               Config{MaxWindowSize: 8192},
		Model:                strategy,
		Tools:                []*Tool{shellTool},
		DownloadInterception: &DownloadGateConfig{Enabled: true},
	})

	ch1, err := ah.Run(context.Background(), "install")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range ch1 {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err == nil {
				interruptID = payload.InterruptId
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected download interrupt")
	}
	if handlerCalls != 0 {
		t.Fatal("handler must not run before approval")
	}

	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"allow-always"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch2 {
	}
	if handlerCalls != 1 {
		t.Fatalf("handlerCalls = %d, want 1", handlerCalls)
	}

	ch3, err := ah.Run(context.Background(), "install again")
	if err != nil {
		t.Fatal(err)
	}
	var sawInterrupt bool
	for ev := range ch3 {
		if ev.Type == StreamEventInterrupt {
			sawInterrupt = true
		}
	}
	if sawInterrupt {
		t.Fatal("allow-always should not re-raise download interrupt")
	}
	if handlerCalls != 2 {
		t.Fatalf("handlerCalls = %d, want 2", handlerCalls)
	}
}

func TestHarness_downloadGate_rejectAlwaysRemembers(t *testing.T) {
	shellTool := NewTool(ToolConfig{
		Name:    "shell",
		Handler: func(ctx context.Context) (string, error) { return "executed", nil },
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			switch {
			case invokeCount == 1 || invokeCount == 3:
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "s1", CallID: "s1", Name: "shell", Arguments: `{"cmd":"npm install foo"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
			default:
				events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			}
		},
	}
	ah := NewAgent(context.Background(), AgentOptions{
		Config:               Config{MaxWindowSize: 8192},
		Model:                strategy,
		Tools:                []*Tool{shellTool},
		DownloadInterception: &DownloadGateConfig{Enabled: true},
	})

	ch1, err := ah.Run(context.Background(), "install")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range ch1 {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err == nil {
				interruptID = payload.InterruptId
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected download interrupt")
	}

	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"reject-always"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch2 {
	}

	ch3, err := ah.Run(context.Background(), "install again")
	if err != nil {
		t.Fatal(err)
	}
	var sawInterrupt bool
	for ev := range ch3 {
		if ev.Type == StreamEventInterrupt {
			sawInterrupt = true
		}
	}
	if sawInterrupt {
		t.Fatal("reject-always should not re-raise download interrupt")
	}
	var denied int
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleTool && strings.Contains(m.Content, "permission denied") {
			denied++
		}
	}
	if denied < 1 {
		t.Fatal("expected permission denied in context")
	}
}

func TestHarness_downloadGate_customPatterns(t *testing.T) {
	var handlerCalls int
	shellTool := NewTool(ToolConfig{
		Name: "shell",
		Handler: func(ctx context.Context) (string, error) {
			handlerCalls++
			return "executed", nil
		},
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "c1", CallID: "c1", Name: "shell", Arguments: `{"cmd":"choco install firefox"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{shellTool},
		DownloadInterception: &DownloadGateConfig{
			Enabled:  true,
			Patterns: []string{`choco\s+install`},
		},
	})

	ch1, err := ah.Run(context.Background(), "install firefox")
	if err != nil {
		t.Fatal(err)
	}
	var interruptID string
	for ev := range ch1 {
		if ev.Type == StreamEventInterrupt {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err == nil {
				interruptID = payload.InterruptId
			}
		}
	}
	if interruptID == "" {
		t.Fatal("expected download interrupt with custom patterns")
	}

	ch2, err := ah.ReturnFromInterrupt(context.Background(), map[string][]byte{
		interruptID: []byte(`{"optionId":"allow-once"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch2 {
	}
	if handlerCalls != 1 {
		t.Fatalf("handlerCalls = %d, want 1", handlerCalls)
	}
}

func TestHarness_downloadGate_noDoubleGating(t *testing.T) {
	shellTool := NewTool(ToolConfig{
		Name:               "shell",
		PermissionRequired: true,
		ApprovalReason:     "shell",
		Handler:            func(ctx context.Context) (string, error) { return "executed", nil },
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "s1", CallID: "s1", Name: "shell", Arguments: `{"cmd":"npm install foo"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	ah := NewAgent(context.Background(), AgentOptions{
		Config:               Config{MaxWindowSize: 8192},
		Model:                strategy,
		Tools:                []*Tool{shellTool},
		DownloadInterception: &DownloadGateConfig{Enabled: true},
	})

	ch1, err := ah.Run(context.Background(), "install")
	if err != nil {
		t.Fatal(err)
	}
	var interruptCount int
	for ev := range ch1 {
		if ev.Type == StreamEventInterrupt {
			interruptCount++
		}
	}
	if interruptCount != 1 {
		t.Fatalf("interruptCount = %d, want 1 (toolPermissionGate only; downloadGate skips)", interruptCount)
	}
}

func TestHarness_downloadGate_respectsExemptTools(t *testing.T) {
	var handlerCalls int
	shellTool := NewTool(ToolConfig{
		Name: "shell",
		Handler: func(ctx context.Context) (string, error) {
			handlerCalls++
			return "executed", nil
		},
	})
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "s1", CallID: "s1", Name: "shell", Arguments: `{"cmd":"npm install foo"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	ah := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  strategy,
		Tools:  []*Tool{shellTool},
		DownloadInterception: &DownloadGateConfig{
			Enabled:     true,
			ExemptTools: []string{"shell"},
		},
	})

	ch, err := ah.Run(context.Background(), "install")
	if err != nil {
		t.Fatal(err)
	}
	var sawInterrupt bool
	for ev := range ch {
		if ev.Type == StreamEventInterrupt {
			sawInterrupt = true
		}
	}
	if sawInterrupt {
		t.Fatal("exempt tool should not trigger download interrupt")
	}
	if handlerCalls != 1 {
		t.Fatalf("handlerCalls = %d, want 1", handlerCalls)
	}
}

func streamEof(ctx context.Context, inv ToolInvocation) (string, error) {
	return "", nil
}
