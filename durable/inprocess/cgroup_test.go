package inprocess

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/cgroup"
	"github.com/ryanaldo34/tacklr/vfs"
)

func newCgroupRuntime(t *testing.T, root string) *Runtime {
	t.Helper()
	return New(Config{
		Catalog:    newCatalog(t, scriptedComplete("hello"), durable.AgentSpec{}),
		Projection: vfs.DirectProjection{},
		CgroupRoot: root,
	})
}

func TestSessionCgroup_lifecycle(t *testing.T) {
	ctx := t.Context()
	root := filepath.Join(t.TempDir(), cgroup.DefaultRootName)
	rt := newCgroupRuntime(t, root)

	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, cgroup.EncodeID(string(id)))
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("session cgroup not created at CreateSession: %v", err)
	}

	if err := rt.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("session cgroup still present after Close: %v", err)
	}
}

func TestSessionCgroup_childSessionsOwnCgroup(t *testing.T) {
	ctx := t.Context()
	root := filepath.Join(t.TempDir(), cgroup.DefaultRootName)
	rt := newCgroupRuntime(t, root)

	parent, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := rt.CreateSession(ctx, durable.CreateSession{
		AgentID:   "default",
		Parent:    parent,
		SessionID: durable.ChildSessionID(parent, "researcher", "c1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	parentDir := filepath.Join(root, cgroup.EncodeID(string(parent)))
	childDir := filepath.Join(root, cgroup.EncodeID(string(child)))
	if parentDir == childDir {
		t.Fatal("child and parent must not share a cgroup")
	}
	for _, dir := range []string{parentDir, childDir} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("session cgroup %q not created: %v", dir, err)
		}
	}

	for _, id := range []durable.SessionID{child, parent} {
		if err := rt.Close(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{parentDir, childDir} {
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("session cgroup %q still present after Close: %v", dir, err)
		}
	}
}

func TestSessionCgroup_cleanupFailureDoesNotBlockClose(t *testing.T) {
	ctx := t.Context()
	root := filepath.Join(t.TempDir(), cgroup.DefaultRootName)
	rt := newCgroupRuntime(t, root)

	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	// A child cgroup left behind (e.g. by a crashed worker) prevents rmdir.
	if err := os.MkdirAll(filepath.Join(root, cgroup.EncodeID(string(id)), "leftover"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := rt.Close(ctx, id); err != nil {
		t.Fatalf("Close must succeed even when cgroup cleanup fails: %v", err)
	}
	if _, err := rt.Status(ctx, id); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Errorf("session must be closed despite cgroup cleanup failure, got err=%v", err)
	}
}

func TestSessionCgroup_gracefulDegradeWhenUnavailable(t *testing.T) {
	ctx := t.Context()
	file := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	rt := newCgroupRuntime(t, filepath.Join(file, cgroup.DefaultRootName))

	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatalf("session creation must not fail when cgroup v2 is unavailable: %v", err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	waitEvents(t, rt, id, sub, 5*time.Second)

	if err := rt.Close(ctx, id); err != nil {
		t.Fatal(err)
	}
}
