package temporal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/cgroup"
)

func TestBindCgroupAttachesActiveSessionWhenEnabled(t *testing.T) {
	root := t.TempDir()
	a := &Activities{cgroups: cgroup.NewManager(root)}

	ctx := a.bindCgroup(context.Background(), "test-session")

	sess := cgroup.FromContext(ctx)
	if sess == nil {
		t.Fatal("bindCgroup left no session in ctx")
	}
	if !sess.IsActive() {
		t.Fatal("session must be active after Create")
	}
	if got := sess.Path(); got != filepath.Join(root, "test-session") {
		t.Fatalf("path = %q, want %q", got, filepath.Join(root, "test-session"))
	}
}

func TestBindCgroupDegradesWhenSubtreeUnusable(t *testing.T) {
	// Point at a FILE so MkdirAll fails: the manager disables and the ctx stays
	// ungrouped (the durable runtime never fails a turn over cgroups).
	file := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	a := &Activities{cgroups: cgroup.NewManager(file)}

	ctx := a.bindCgroup(context.Background(), "test-session")

	if cgroup.FromContext(ctx) != nil {
		t.Fatal("must run ungrouped when the subtree is unusable")
	}
}

func TestBindCgroupNilManagerIsNoOp(t *testing.T) {
	a := &Activities{}
	if cgroup.FromContext(a.bindCgroup(context.Background(), "x")) != nil {
		t.Fatal("nil manager must not bind")
	}
}

// TestCloseDerivedSessionRemovesCgroup covers closeCgroupTree's per-id leaf:
// Close works on a derived handle (child ids never passed through
// CreateSession) and is a no-op for never-provisioned ids.
func TestCloseDerivedSessionRemovesCgroup(t *testing.T) {
	ctx := context.Background()
	m := cgroup.NewManager(t.TempDir())

	// A child id contains '/' so EncodeID escapes it — the derived handle must
	// still resolve to the dir the activity's Create provisioned.
	childID := "parent/w/researcher/c1"
	sess, err := m.Create(ctx, childID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sess.Path()); err != nil {
		t.Fatalf("cgroup not created: %v", err)
	}
	if err := m.Close(m.Session(childID)); err != nil {
		t.Fatalf("close derived handle: %v", err)
	}
	if _, err := os.Stat(sess.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("derived close left the dir behind: %v", err)
	}

	// A child that never ran an activity has no dir; closing it is a no-op.
	if err := m.Close(m.Session("never-created")); err != nil {
		t.Fatalf("close never-created must be nil, got %v", err)
	}
}
