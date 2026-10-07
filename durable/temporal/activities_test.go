package temporal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestActivities_directCallPersistsUserState(t *testing.T) {
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
	}), MaxWindowSize: 8192}
	snaps := durable.NewMemorySnapshot()
	log := durable.NewMemoryEventLog()
	acts := &activities{Agent: agent, Snapshots: snaps, Fallback: log, DisableStreams: true, Secrets: durable.NewMemorySecretStorage()}
	out, err := acts.Inference(t.Context(), durable.InferenceInput{
		SessionID: "s", Rec: durable.Snapshot{},
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
}

func TestActivities_childTurnUsesParentSecrets(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-parent"), 0o644); err != nil {
		t.Fatal(err)
	}
	var gotToken string
	open := vfs.Tree(vfs.At("docs", vfs.Local(dir)))
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
	}), MaxWindowSize: 8192,

		OpenVFS: func(ctx context.Context, sessionID string, req vfs.Request) (*vfs.MountSession, error) {
			if len(req.Bindings) > 0 {
				gotToken = req.Bindings[0].Auth.Token
			}
			return open(ctx, sessionID, req)
		}}
	store := durable.NewMemorySecretStorage()
	parentAuth := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "parent-tok"},
	}}}
	if err := store.Put(t.Context(), "parent", durable.Secrets{Auth: parentAuth}); err != nil {
		t.Fatal(err)
	}
	acts := newActs(agent, durable.NewMemoryEventLog(), true)
	acts.Secrets = store
	if _, err := acts.Inference(t.Context(), durable.InferenceInput{
		SessionID: "child",
		Rec: durable.Snapshot{
			Parent: "parent",
			Mounts: durable.ApplyAuth(nil, parentAuth),
		},
		User: &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
	}); err != nil {
		t.Fatal(err)
	}
	if gotToken != "parent-tok" {
		t.Fatalf("OpenVFS token=%q", gotToken)
	}
}
