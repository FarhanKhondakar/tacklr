package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/testkit"
)

// healthProtocol is a host server.Protocol with one HTTP route (no Runtime turns).
type healthProtocol struct{}

func (healthProtocol) HandleInbound(ctx context.Context, env server.ProtocolEnv, body []byte) error {
	return nil
}

func (healthProtocol) HTTPRoutes() []server.HTTPRoute {
	return []server.HTTPRoute{{
		Method:  "GET",
		Pattern: "/healthz",
		Handler: func(env server.ProtocolEnv, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		},
	}}
}

func (healthProtocol) OnStreamEvent(ctx context.Context, env server.ProtocolEnv, threadID string, ev tacklr.StreamEvent, reqID json.RawMessage) server.StreamControl {
	return server.StreamControl{Finished: true}
}

func (healthProtocol) OnStreamClosed(ctx context.Context, env server.ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	return nil
}

func TestServer_mountsHostProtocolBesideACP(t *testing.T) {
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	srv := server.NewServer(k.Runtime, k.Catalog, acp.New(nil), healthProtocol{}).AllowAnonymousNetwork()
	mux := srv.HTTPMux()

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("host route = %d %q", rec.Code, rec.Body.String())
	}

	acpRec := httptest.NewRecorder()
	mux.ServeHTTP(acpRec, httptest.NewRequest(http.MethodGet, "/acp", nil))
	if acpRec.Code != http.StatusUpgradeRequired {
		t.Fatalf("acp GET without upgrade = %d %s", acpRec.Code, acpRec.Body.String())
	}
}

func TestACPProtocol_initializeResultShape(t *testing.T) {
	result := acp.InitializeResult(nil, 1, nil, false)
	if result["protocolVersion"] != 1 {
		t.Fatalf("protocolVersion = %v", result["protocolVersion"])
	}
	// Client asks for a future major; we respond with the latest we support (1).
	if v := acp.InitializeResult(nil, 99, nil, false)["protocolVersion"]; v != 1 {
		t.Fatalf("negotiated version for client 99 = %v, want 1", v)
	}
	caps, ok := result["agentCapabilities"].(map[string]any)
	if !ok {
		t.Fatal("missing agentCapabilities")
	}
	mcpCaps, ok := caps["mcpCapabilities"].(map[string]any)
	if !ok || mcpCaps["http"] != true {
		t.Fatalf("mcpCapabilities = %v", mcpCaps)
	}
	pc, ok := caps["promptCapabilities"].(map[string]any)
	if !ok || pc["image"] != false {
		t.Fatalf("nil catalog should advertise image=false, got %v", pc)
	}
	if pc["embeddedContext"] != true || pc["audio"] != false {
		t.Fatalf("promptCapabilities = %v", pc)
	}
	capMeta, _ := caps["_meta"].(map[string]any)
	tacklrCap, _ := capMeta["tacklr"].(map[string]any)
	vfsCap, _ := tacklrCap["vfs"].(map[string]any)
	if vfsCap["credentials"] != true || vfsCap["tokenRefresh"] != true {
		t.Fatalf("agentCapabilities._meta.tacklr.vfs = %#v", vfsCap)
	}
	info, ok := result["agentInfo"].(map[string]string)
	if !ok || info["name"] == "" {
		t.Fatalf("agentInfo = %v", result["agentInfo"])
	}
	meta, _ := result["_meta"].(map[string]any)
	tacklrMeta, _ := meta["tacklr"].(map[string]any)
	transports, _ := tacklrMeta["transports"].([]string)
	if len(transports) != 1 || transports[0] != "websocket" {
		t.Fatalf("transports = %v, want [websocket]", transports)
	}
}

type pumpProto struct {
	onEvent func(tacklr.StreamEvent) server.StreamControl
}

func (pumpProto) HandleInbound(context.Context, server.ProtocolEnv, []byte) error {
	return nil
}
func (pumpProto) HTTPRoutes() []server.HTTPRoute { return nil }
func (p pumpProto) OnStreamEvent(ctx context.Context, env server.ProtocolEnv, threadID string, ev tacklr.StreamEvent, reqID json.RawMessage) server.StreamControl {
	if p.onEvent != nil {
		return p.onEvent(ev)
	}
	return server.StreamControl{Finished: true}
}
func (pumpProto) OnStreamClosed(context.Context, server.ProtocolEnv, string, json.RawMessage, bool) error {
	return nil
}

func terminalControl(ev tacklr.StreamEvent) server.StreamControl {
	if ev.Type == tacklr.StreamEventComplete || ev.Type == tacklr.StreamEventError {
		return server.StreamControl{Finished: true}
	}
	return server.StreamControl{}
}

// TestRunTurn_midPromptCancelThenNextPrompt is the protocol-agnostic coverage
// for Runtime.Cancel during a turn: the pump finishes, then the next prompt
// on the same session completes with new content (no leftover cancel).
func TestRunTurn_midPromptCancelThenNextPrompt(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	strategy := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			startedOnce.Do(func() { close(started) })
			for {
				select {
				case <-ctx.Done():
					return
				case ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventMessage, Content: "early", IsComplete: false,
				}:
				}
			}
		},
	}
	k := newTestRuntime(t, strategy, durable.AgentSpec{})
	ctx := t.Context()
	id, err := k.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	env := server.ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog}

	var sawStream atomic.Bool
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- server.RunTurn(ctx, env, pumpProto{onEvent: func(ev tacklr.StreamEvent) server.StreamControl {
			if ev.Type == tacklr.StreamEventMessage {
				sawStream.Store(true)
			}
			return terminalControl(ev)
		}}, string(id), nil, server.PromptOrResume{Prompt: durable.Prompt{Text: "hi"}})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !sawStream.Load() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for first stream event")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := k.Runtime.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-firstDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("first turn: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first turn did not finish after cancel")
	}

	strategy.InvokeFn = func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventMessage, Content: "after-cancel", IsComplete: true,
		}
	}
	var second []tacklr.StreamEvent
	if err := server.RunTurn(ctx, env, pumpProto{onEvent: func(ev tacklr.StreamEvent) server.StreamControl {
		second = append(second, ev)
		return terminalControl(ev)
	}}, string(id), nil, server.PromptOrResume{Prompt: durable.Prompt{Text: "again"}}); err != nil {
		t.Fatalf("second turn: %v", err)
	}
	var sawAfter, complete bool
	for _, ev := range second {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "after-cancel") {
			sawAfter = true
		}
		if ev.Type == tacklr.StreamEventComplete {
			complete = true
		}
	}
	if !sawAfter || !complete {
		t.Fatalf("want after-cancel + complete, got %+v", second)
	}
}

func TestRunTurn_runtimeErrors(t *testing.T) {
	k := newTestRuntime(t, &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "x", IsComplete: true}
		},
	}, durable.AgentSpec{})
	env := server.ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog}
	if err := server.RunTurn(t.Context(), env, pumpProto{}, "missing", nil, server.PromptOrResume{Prompt: durable.Prompt{Text: "hi"}}); err == nil {
		t.Fatal("want subscribe missing session")
	}
	id, err := k.Runtime.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RunTurn(t.Context(), env, pumpProto{}, string(id), nil, server.PromptOrResume{
		Prompt: durable.Prompt{Text: "hi", State: map[string]any{"ch": make(chan int)}},
	}); err == nil {
		t.Fatal("want prompt encode failure")
	}
	if err := server.RunTurn(t.Context(), env, pumpProto{}, string(id), nil, server.PromptOrResume{
		Resume: &durable.Resume{State: map[string]any{"ch": make(chan int)}},
	}); err == nil {
		t.Fatal("want resume encode failure")
	}
	id2, err := k.Runtime.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RunTurn(t.Context(), env, pumpProto{onEvent: func(tacklr.StreamEvent) server.StreamControl {
		_ = k.Runtime.Close(t.Context(), id2)
		return server.StreamControl{Resume: map[string][]byte{"nope": []byte(`{}`)}}
	}}, string(id2), nil, server.PromptOrResume{Prompt: durable.Prompt{Text: "hi"}}); err == nil {
		t.Fatal("want resume-after-close failure")
	}
}

func TestRunTurn_protocolErrorStopsTurn(t *testing.T) {
	k := newTestRuntime(t, &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "x", IsComplete: true}
		},
	}, durable.AgentSpec{})
	id, err := k.Runtime.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	err = server.RunTurn(t.Context(), server.ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog}, pumpProto{onEvent: func(tacklr.StreamEvent) server.StreamControl {
		return server.StreamControl{Err: errors.New("encode")}
	}}, string(id), nil, server.PromptOrResume{Prompt: durable.Prompt{Text: "hi"}})
	if err == nil {
		t.Fatal("want protocol error")
	}
}

func TestServeHTTP_respectsContextCancel(t *testing.T) {
	r := newTestRuntime(t, &testkit.ScriptedModel{}, durable.AgentSpec{})
	srv := server.NewServer(r.Runtime, r.Catalog, acp.New(nil)).AllowAnonymousNetwork()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ServeHTTP(ctx, "127.0.0.1:0") }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server.ServeHTTP did not exit")
	}
}

func TestMain(m *testing.M) {
	shutdown, err := telemetry.Init(context.Background(), telemetry.Config{})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = shutdown(context.Background())
	os.Exit(code)
}

type testRuntime struct {
	Runtime durable.Runtime
	Catalog *durable.MemoryCatalog
}

func newTestRuntime(t *testing.T, model tacklr.InferenceStrategy, spec durable.AgentSpec) *testRuntime {
	t.Helper()
	if spec.Options.Model == nil {
		spec.Options.Model = model
	}
	if spec.Options.Model == nil {
		spec.Options.Model = &testkit.ScriptedModel{}
	}
	if spec.Options.Config.MaxWindowSize == 0 {
		spec.Options.Config.MaxWindowSize = 8192
	}
	if spec.Options.Config.SystemPrompt == "" {
		spec.Options.Config.SystemPrompt = "test prompt"
	}
	cat := durable.NewCatalog("default")
	cat.Register("default", spec)
	return &testRuntime{
		Runtime: inprocess.New(inprocess.Config{Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Projection: vfs.DirectProjection{}}),
		Catalog: cat,
	}
}

func newTestServer(t *testing.T) *server.Server {
	t.Helper()
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	return server.NewServer(k.Runtime, k.Catalog, acp.New(nil))
}

type recordingMessageWriter = testkit.RecordingWriter

func acpSessionID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["error"] != nil {
		t.Fatalf("unexpected error: %v", resp["error"])
	}
	sessionID, ok := resp["result"].(map[string]any)["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("missing sessionId in result: %v", resp)
	}
	return sessionID
}

func serveACPInbound(t *testing.T, r *testRuntime, proto server.Protocol, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mw := acp.HTTPWriter(rec)
	env := server.ProtocolEnv{Runtime: r.Runtime, Catalog: r.Catalog, Conn: &server.Conn{Writer: mw}}
	_ = proto.HandleInbound(t.Context(), env, []byte(body))
	return rec
}

type acpTestServer struct {
	t     *testing.T
	r     *testRuntime
	proto server.Protocol
	wire  server.ProtocolWireStore
}

func newACPTestServerWithWire(t *testing.T, r *testRuntime, wire server.ProtocolWireStore) *acpTestServer {
	t.Helper()
	return &acpTestServer{t: t, r: r, proto: acp.New(wire), wire: wire}
}

func (s *acpTestServer) rpc(body string) *httptest.ResponseRecorder {
	s.t.Helper()
	return serveACPInbound(s.t, s.r, s.proto, body)
}

func acpRPCResult(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	if errObj, ok := resp["error"]; ok && errObj != nil {
		t.Fatalf("unexpected error: %v", errObj)
	}
	res, _ := resp["result"].(map[string]any)
	return res
}

func parseACPFrames(t *testing.T, body io.Reader) []map[string]any {
	t.Helper()
	var frames []map[string]any
	dec := json.NewDecoder(body)
	for {
		var frame map[string]any
		if err := dec.Decode(&frame); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode ACP frame: %v", err)
		}
		frames = append(frames, frame)
	}
	return frames
}
