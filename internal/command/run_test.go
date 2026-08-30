package command

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/cgroup"
)

func TestRun_truncatesOversizedOutput(t *testing.T) {
	dir := t.TempDir()
	big := bytes.Repeat([]byte("a"), outputCap+64)
	if err := os.WriteFile(filepath.Join(dir, "big.txt"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := Run(context.Background(), dir, "cat big.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "truncated=true") || !strings.Contains(out, "output truncated") {
		t.Fatalf("want truncated result, got %q", out[:min(len(out), 200)])
	}
}

func TestOutputBudget_emptyAndExhaustedWrites(t *testing.T) {
	b := outputBudget{remaining: 4}
	w := b.writer()
	n, err := w.Write(nil)
	if err != nil || n != 0 || b.truncated {
		t.Fatalf("empty write: n=%d err=%v truncated=%v", n, err, b.truncated)
	}
	n, err = w.Write([]byte("hello"))
	if err != nil || n != 5 || !b.truncated || b.remaining != 0 {
		t.Fatalf("partial: n=%d err=%v remaining=%d truncated=%v", n, err, b.remaining, b.truncated)
	}
	n, err = w.Write([]byte("more"))
	if err != nil || n != 4 || !b.truncated {
		t.Fatalf("exhausted: n=%d err=%v truncated=%v", n, err, b.truncated)
	}
}

func TestSessionCgroupFd_noSessionInContext(t *testing.T) {
	if got := sessionCgroupFd(context.Background()); got != nil {
		t.Fatalf("sessionCgroupFd without a session = %+v; want nil", got)
	}
	if got := sessionCgroupFd(cgroup.WithSession(context.Background(), nil)); got != nil {
		t.Fatalf("sessionCgroupFd with nil session = %+v; want nil", got)
	}
}

func TestSessionCgroupFd_inactiveSession(t *testing.T) {
	root := filepath.Join(t.TempDir(), cgroup.DefaultRootName)
	mgr := cgroup.NewManager(root)
	sess := mgr.Session("sess-1")
	if got := sessionCgroupFd(cgroup.WithSession(context.Background(), sess)); got != nil {
		t.Fatalf("sessionCgroupFd on a disabled manager = %+v; want nil", got)
	}
}

func TestSessionCgroupFd_missingDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), cgroup.DefaultRootName)
	mgr := cgroup.NewManager(root)
	sess, err := mgr.Create(context.Background(), "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	// The cgroup dir was created; delete it so the open fails and the command
	// degrades to running ungrouped.
	if err := os.RemoveAll(sess.Path()); err != nil {
		t.Fatal(err)
	}
	if got := sessionCgroupFd(cgroup.WithSession(context.Background(), sess)); got != nil {
		t.Fatalf("sessionCgroupFd with a missing dir = %+v; want nil", got)
	}
}

func TestSessionCgroupFd_opensActiveSessionDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), cgroup.DefaultRootName)
	mgr := cgroup.NewManager(root)
	sess, err := mgr.Create(context.Background(), "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	got := sessionCgroupFd(cgroup.WithSession(context.Background(), sess))
	if got == nil || got.fd <= 0 {
		t.Fatalf("sessionCgroupFd = %+v; want an open fd", got)
	}
	closed := false
	orig := got.close
	got.close = func() { closed = true; orig() }
	got.close()
	if !closed {
		t.Fatal("close callback did not run")
	}
}
