package temporal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestActivities_unknownAgentAndDirectCall(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	snaps := inprocess.NewMemorySnapshot()
	log := inprocess.NewMemoryEventLog()
	acts := &activities{Catalog: cat, Snapshots: snaps, Fallback: log, DisableStreams: true, Secrets: durable.NewMemorySecretStorage()}
	_, err := acts.Inference(t.Context(), inferenceInput{SessionID: "s", Rec: durable.Snapshot{AgentID: "nope"}})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("missing agent: %v", err)
	}
	_, err = acts.Tool(t.Context(), toolInput{SessionID: "s", Rec: durable.Snapshot{AgentID: "nope"}, Call: tacklr.ToolCall{ID: "c", Name: "x"}})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("tool missing agent: %v", err)
	}
	out, err := acts.Inference(t.Context(), inferenceInput{
		SessionID: "s", Rec: durable.Snapshot{AgentID: "default"},
		User:  &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
		State: map[string]any{"user": "Ryan"},
	})
	if err != nil || !out.Complete {
		t.Fatalf("direct inference: %+v %v", out, err)
	}
	snap, _, err := snaps.Load(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.Checkpoint.UserState()["user"]) != `"Ryan"` {
		t.Fatalf("userState=%s", snap.Checkpoint.UserState()["user"])
	}
	if snap.AgentID != "default" {
		t.Fatalf("snapshot agent=%q", snap.AgentID)
	}
}

func TestActivities_childTurnUsesParentSecrets(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-parent"), 0o644); err != nil {
		t.Fatal(err)
	}
	var gotToken string
	open := vfs.Tree(vfs.At("docs", vfs.Local(dir)))
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
		OpenVFS: func(ctx context.Context, sessionID string, req vfs.Request) (*vfs.MountSession, error) {
			if len(req.Bindings) > 0 {
				gotToken = req.Bindings[0].Auth.Token
			}
			return open(ctx, sessionID, req)
		},
	})
	store := durable.NewMemorySecretStorage()
	parentAuth := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "parent-tok"},
	}}}
	if err := store.Put(t.Context(), "parent", durable.Secrets{Auth: parentAuth}); err != nil {
		t.Fatal(err)
	}
	acts := newActs(cat, inprocess.NewMemoryEventLog(), true)
	acts.Secrets = store
	if _, err := acts.Inference(t.Context(), inferenceInput{
		SessionID: "child",
		Rec: durable.Snapshot{
			Parent:  "parent",
			AgentID: "default",
			Mounts:  adapter.ApplyAuth(nil, parentAuth),
		},
		User: &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
	}); err != nil {
		t.Fatal(err)
	}
	if gotToken != "parent-tok" {
		t.Fatalf("OpenVFS token=%q", gotToken)
	}
}

func TestActivityError_splitsRetry(t *testing.T) {
	ctx := t.Context()
	if err := activityError(ctx, nil); err != nil {
		t.Fatalf("nil: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := activityError(canceled, tacklr.ErrMaxTokens); !temporal.IsCanceledError(err) {
		t.Fatalf("cancel: %v", err)
	}
	deadline, stop := context.WithTimeout(ctx, 0)
	defer stop()
	<-deadline.Done()
	got := activityError(deadline, context.DeadlineExceeded)
	var deadApp *temporal.ApplicationError
	if temporal.IsCanceledError(got) || !errors.As(got, &deadApp) || !deadApp.NonRetryable() {
		t.Fatalf("deadline: %v", got)
	}

	permanent := []error{
		tacklr.ErrApiKeyNotSet,
		tacklr.ErrMaxTokens,
		errors.New("connection reset"),
	}
	for _, src := range permanent {
		err := activityError(ctx, src)
		var app *temporal.ApplicationError
		if !errors.As(err, &app) || !app.NonRetryable() || !errors.Is(err, src) {
			t.Fatalf("stop %v → %v", src, err)
		}
	}

	retryable := []error{
		tacklr.Network(errors.New("connection reset")),
		tacklr.ErrModelRefused,
		fmt.Errorf("%w: %w", tacklr.ErrModelAfterTools, tacklr.ErrModelRefused),
		durable.ErrStaleCheckpoint,
		fmt.Errorf("save: %w: %w", durable.ErrStaleCheckpoint, tacklr.ErrInvalid),
	}
	for _, src := range retryable {
		err := activityError(ctx, src)
		var app *temporal.ApplicationError
		if errors.As(err, &app) && app.NonRetryable() {
			t.Fatalf("retryable marked permanent: %v", err)
		}
		if !errors.Is(err, src) && err.Error() != src.Error() {
			t.Fatalf("retryable %v → %v", src, err)
		}
	}
}

type saveErrStore struct {
	durable.SnapshotStore
	err error
}

func (s saveErrStore) Save(context.Context, durable.SessionID, durable.Snapshot, durable.Revision) (durable.Revision, error) {
	return "", s.err
}

func TestTool_saveErrorKeepsRetrySplit(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Model:  &testkit.ScriptedModel{},
			Config: tacklr.Config{MaxWindowSize: 8192},
			Tools: []*tacklr.Tool{
				tacklr.NewTool(tacklr.ToolConfig{
					Name: "boom",
					Handler: func(context.Context) (string, error) {
						return "", tacklr.ErrFailed
					},
				}),
			},
		},
	})
	snaps := inprocess.NewMemorySnapshot()
	acts := &activities{
		Catalog: cat, Snapshots: saveErrStore{SnapshotStore: snaps, err: tacklr.Network(errors.New("db down"))},
		Fallback: inprocess.NewMemoryEventLog(), DisableStreams: true,
		Secrets: durable.NewMemorySecretStorage(),
	}
	_, err := acts.Tool(t.Context(), toolInput{
		SessionID: "s",
		Rec:       durable.Snapshot{AgentID: "default"},
		Call:      tacklr.ToolCall{ID: "c", Name: "boom", Arguments: "{}"},
	})
	var app *temporal.ApplicationError
	if err == nil || (errors.As(err, &app) && app.NonRetryable()) || !errors.Is(err, tacklr.ErrFailed) {
		t.Fatalf("retryable persist: %v", err)
	}

	acts.Snapshots = saveErrStore{SnapshotStore: snaps, err: tacklr.ErrNotFound}
	_, err = acts.Tool(t.Context(), toolInput{
		SessionID: "s",
		Rec:       durable.Snapshot{AgentID: "default"},
		Call:      tacklr.ToolCall{ID: "c2", Name: "boom", Arguments: "{}"},
	})
	if !errors.As(err, &app) || !app.NonRetryable() || !errors.Is(err, tacklr.ErrNotFound) {
		t.Fatalf("permanent persist: %v", err)
	}
}

func TestRunJob_missingIsPermanent(t *testing.T) {
	acts := &activities{Secrets: durable.NewMemorySecretStorage()}
	_, err := acts.RunJob(t.Context(), runJobInput{Name: "missing"})
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || !app.NonRetryable() || !errors.Is(err, tacklr.ErrNotFound) {
		t.Fatalf("nil jobs: %v", err)
	}
	acts.Jobs = map[string]durable.JobHandler{}
	_, err = acts.RunJob(t.Context(), runJobInput{Name: "missing"})
	if !errors.As(err, &app) || !app.NonRetryable() || !errors.Is(err, tacklr.ErrNotFound) {
		t.Fatalf("unknown job: %v", err)
	}
	acts.Jobs["ok"] = func(context.Context, string) (string, error) {
		return "", tacklr.Network(errors.New("later"))
	}
	_, err = acts.RunJob(t.Context(), runJobInput{Name: "ok"})
	if err == nil || !errors.Is(err, tacklr.ErrNetwork) || errors.As(err, &app) && app.NonRetryable() {
		t.Fatalf("handler: %v", err)
	}
}
