package cgroup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func newTempManager(t *testing.T) (*Manager, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), DefaultRootName)
	return NewManager(root), root
}

func TestCreate_provisionsSessionDir(t *testing.T) {
	m, root := newTempManager(t)
	s, err := m.Create(context.Background(), "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if !m.Enabled() {
		t.Fatal("manager should be enabled after a successful create")
	}
	dir := filepath.Join(root, EncodeID("sess-1"))
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("session cgroup dir missing: %v", err)
	}
	if s.Path() != dir || s.ID() != "sess-1" || s.Name() != EncodeID("sess-1") {
		t.Fatalf("session handle mismatch: %+v", s)
	}
}

func TestCreate_degradesWhenSubtreeUnwritable(t *testing.T) {
	file := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(filepath.Join(file, DefaultRootName))
	_, err := m.Create(context.Background(), "sess-1")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Create = %v; want ErrUnavailable", err)
	}
	if m.Enabled() {
		t.Fatal("manager must stay disabled when the subtree cannot be created")
	}
}

func TestCreate_degradesWhenSubtreeReadOnly(t *testing.T) {
	root := filepath.Join(t.TempDir(), DefaultRootName)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	probe := filepath.Join(root, ".tacklr-probe")
	if err := os.MkdirAll(probe, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(probe, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(probe, 0o755) })
	// Root bypasses permission bits; the read-only premise does not hold.
	if err := os.MkdirAll(filepath.Join(probe, ".still-writable"), 0o755); err == nil {
		t.Skip("running as root; permission bits do not block writes")
	}
	m := NewManager(probe)
	if _, err := m.Create(context.Background(), "sess-1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Create under unwritable subtree = %v; want ErrUnavailable", err)
	}
}

func TestClose_removesSessionDir(t *testing.T) {
	m, root := newTempManager(t)
	s, err := m.Create(context.Background(), "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(s); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("session cgroup dir still present after Close: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Errorf("harness subtree must outlive a session Close: %v", err)
	}
}

func TestClose_killsSurvivors(t *testing.T) {
	m, _ := newTempManager(t)
	s, err := m.Create(context.Background(), "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.Path(), "cgroup.procs"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(s); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("session cgroup dir still present after Close: %v", err)
	}
}

func TestClose_returnsErrorWhenRemoveFails(t *testing.T) {
	m, _ := newTempManager(t)
	s, err := m.Create(context.Background(), "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	// A non-empty directory cannot be removed by os.Remove.
	if err := os.MkdirAll(filepath.Join(s.Path(), "blocker"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(s); err == nil {
		t.Fatal("Close should error when the cgroup dir cannot be removed")
	}
}

func TestClose_nilAndDisabledNoop(t *testing.T) {
	m, _ := newTempManager(t)
	if err := m.Close(nil); err != nil {
		t.Fatalf("Close(nil) = %v; want nil", err)
	}
	file := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	disabled := NewManager(filepath.Join(file, DefaultRootName))
	sess := disabled.Session("sess-1")
	if err := disabled.Close(sess); err != nil {
		t.Fatalf("Close on disabled manager = %v; want nil", err)
	}
}

func TestClose_alreadyRemovedIsIdempotent(t *testing.T) {
	m, _ := newTempManager(t)
	s, err := m.Create(context.Background(), "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(s); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(s); err != nil {
		t.Fatalf("second Close = %v; want nil", err)
	}
}

func TestReap_cleansOrphans(t *testing.T) {
	m, root := newTempManager(t)
	for _, id := range []string{"orphan-1", "orphan-2"} {
		if _, err := m.Create(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	m.Reap(context.Background())
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("Reap left %d entries behind", len(entries))
	}
}

func TestNewManager_defaultRoot(t *testing.T) {
	m := NewManager("")
	if m.root != DefaultRoot {
		t.Errorf("NewManager(\"\") root = %q; want %q", m.root, DefaultRoot)
	}
	if m.Enabled() {
		t.Error("fresh manager must not report enabled before probing")
	}
}

func TestCreate_degradesWhenSessionDirBlockedByFile(t *testing.T) {
	m, root := newTempManager(t)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, EncodeID("sess-1")), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(context.Background(), "sess-1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Create with blocked session dir = %v; want ErrUnavailable", err)
	}
}

func TestCreate_repeatedCallAfterDisable(t *testing.T) {
	file := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(filepath.Join(file, DefaultRootName))
	if _, err := m.Create(context.Background(), "sess-1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("first Create = %v; want ErrUnavailable", err)
	}
	if _, err := m.Create(context.Background(), "sess-2"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("second Create = %v; want ErrUnavailable (disabled state short-circuit)", err)
	}
}

func TestReap_disabledIsNoop(t *testing.T) {
	file := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(filepath.Join(file, DefaultRootName))
	m.Reap(context.Background())
}

func TestReap_skipsFilesAndHandlesMissingRoot(t *testing.T) {
	m, root := newTempManager(t)
	if _, err := m.Create(context.Background(), "orphan-1"); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(root, EncodeID("orphan-2"))
	if err := os.MkdirAll(filepath.Join(blocked, "blocker"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "junk-file"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	m.Reap(context.Background())
	if _, err := os.Stat(filepath.Join(root, "junk-file")); err != nil {
		t.Errorf("Reap must not remove non-directory entries: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, EncodeID("orphan-1"))); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Reap left the orphan cgroup behind: %v", err)
	}
	// A cgroup that cannot be rmdir'd (child cgroup still present) is left
	// in place and warned about, not silently dropped.
	if _, err := os.Stat(blocked); err != nil {
		t.Errorf("Reap must leave non-removable cgroups for a later pass: %v", err)
	}

	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	m.Reap(context.Background())
}

func TestWithSessionFromContext(t *testing.T) {
	m, _ := newTempManager(t)
	s := m.Session("sess-1")
	if got := FromContext(context.Background()); got != nil {
		t.Fatalf("FromContext(empty) = %+v; want nil", got)
	}
	if got := FromContext(WithSession(context.Background(), s)); got != s {
		t.Fatalf("FromContext(WithSession) = %+v; want %+v", got, s)
	}
	if got := FromContext(WithSession(context.Background(), nil)); got != nil {
		t.Fatalf("FromContext(WithSession(nil)) = %+v; want nil", got)
	}
}

func TestSession_IsActive(t *testing.T) {
	m, _ := newTempManager(t)
	if m.Session("sess-1").IsActive() {
		t.Fatal("session must be inactive before the manager is probed")
	}
	if _, err := m.Create(context.Background(), "sess-1"); err != nil {
		t.Fatal(err)
	}
	if !m.Session("sess-2").IsActive() {
		t.Fatal("session must be active once the manager is enabled")
	}
	var nilSess *Session
	if nilSess.IsActive() {
		t.Fatal("nil session must be inactive")
	}
}
