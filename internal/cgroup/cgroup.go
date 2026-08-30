package cgroup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// ErrUnavailable means cgroup v2 cannot be used on this host (unmounted,
// unwritable, or rootless without a delegated subtree).
var ErrUnavailable = errors.New("cgroup v2 unavailable")

// DefaultRoot is the default harness subtree on the cgroup v2 filesystem.
const DefaultRoot = "/sys/fs/cgroup/harness"

// filesystem is the narrow set of filesystem operations the manager needs.
// The real implementation wraps os over cgroupfs; tests run against temp dirs.
type filesystem interface {
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(name string, data []byte, perm os.FileMode) error
	ReadDir(name string) ([]os.DirEntry, error)
	Remove(path string) error
}

type osfs struct{}

func (osfs) MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }
func (osfs) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}
func (osfs) ReadDir(name string) ([]os.DirEntry, error) { return os.ReadDir(name) }
func (osfs) Remove(path string) error                   { return os.Remove(path) }

// Manager owns the harness cgroup subtree and the per-session lifecycle. It
// degrades to a no-op when the subtree cannot be created or written.
type Manager struct {
	fs   filesystem
	root string
	log  *slog.Logger

	mu       sync.Mutex
	state    int
	warnOnce sync.Once
}

const (
	stateUnknown = iota
	stateEnabled
	stateDisabled
)

// NewManager builds a Manager for root (default DefaultRoot). root is the
// harness subtree; session cgroups are created directly under it.
func NewManager(root string) *Manager {
	if root == "" {
		root = DefaultRoot
	}
	return &Manager{
		fs:   osfs{},
		root: root,
		log:  slog.Default(),
	}
}

// Enabled reports whether the subtree has been probed writable.
func (m *Manager) Enabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state == stateEnabled
}

// Session returns a handle for id without touching the filesystem.
func (m *Manager) Session(id string) *Session {
	name := EncodeID(id)
	return &Session{m: m, id: id, name: name, path: filepath.Join(m.root, name)}
}

// Create provisions the cgroup for id. It returns ErrUnavailable (wrapped)
// when the subtree cannot be created or written, so callers can degrade.
func (m *Manager) Create(ctx context.Context, id string) (*Session, error) {
	if err := m.ensure(); err != nil {
		return nil, err
	}
	s := m.Session(id)
	if err := m.fs.MkdirAll(s.path, 0o755); err != nil {
		return nil, m.disable(fmt.Errorf("create cgroup for session %q: %w", id, err))
	}
	slog.DebugContext(ctx, "session cgroup created", "session_id", id, "path", s.path)
	return s, nil
}

// Close terminates surviving processes and removes the session cgroup. It is
// safe to call more than once and on a disabled Manager.
func (m *Manager) Close(s *Session) error {
	if s == nil || s.path == "" {
		return nil
	}
	if !m.Enabled() {
		return nil
	}
	_ = m.fs.WriteFile(filepath.Join(s.path, "cgroup.kill"), []byte("1"), 0o600)
	if err := removeCgroupDir(m.fs, s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove cgroup for session %q: %w", s.id, err)
	}
	return nil
}

// removeCgroupDir rmdirs a session cgroup. cgroupfs rmdir only needs the
// cgroup empty of processes and child cgroups; the pseudo-files (cgroup.kill,
// cgroup.procs) are dropped first so the same call works on plain temp
// filesystems in tests. Unlinking a cgroupfs pseudo-file is a harmless
// permission error that is ignored.
func removeCgroupDir(fs filesystem, path string) error {
	_ = fs.Remove(filepath.Join(path, "cgroup.kill"))
	_ = fs.Remove(filepath.Join(path, "cgroup.procs"))
	return fs.Remove(path)
}

// Reap sweeps the subtree, killing and removing every session cgroup. Call it
// at startup to clean orphans left by a crashed harness. Do not call it while
// sessions are live.
func (m *Manager) Reap(ctx context.Context) {
	if err := m.ensure(); err != nil {
		return
	}
	entries, _ := m.fs.ReadDir(m.root)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(m.root, entry.Name())
		_ = m.fs.WriteFile(filepath.Join(path, "cgroup.kill"), []byte("1"), 0o600)
		if err := removeCgroupDir(m.fs, path); err != nil {
			slog.WarnContext(ctx, "cgroup reap failed", "path", path, "error", err)
		}
	}
}

func (m *Manager) ensure() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.state {
	case stateEnabled:
		return nil
	case stateDisabled:
		return ErrUnavailable
	}
	if err := m.fs.MkdirAll(m.root, 0o755); err != nil {
		return m.disableLocked(fmt.Errorf("create harness subtree: %w", err))
	}
	probe := filepath.Join(m.root, ".tacklr-probe")
	if err := m.fs.MkdirAll(probe, 0o755); err != nil {
		return m.disableLocked(fmt.Errorf("probe harness subtree: %w", err))
	}
	_ = m.fs.Remove(probe)
	m.state = stateEnabled
	return nil
}

func (m *Manager) disable(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.disableLocked(err)
}

// disableLocked marks the manager disabled and logs once. Callers must hold m.mu.
func (m *Manager) disableLocked(err error) error {
	m.state = stateDisabled
	m.warnOnce.Do(func() {
		m.log.Warn("cgroup v2 unavailable; harness sessions will not be process-isolated", "root", m.root, "error", err)
	})
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}

// Session is one harness session's cgroup handle.
type Session struct {
	m    *Manager
	id   string
	name string
	path string
}

// ID is the harness session id this cgroup belongs to.
func (s *Session) ID() string { return s.id }

// Name is the encoded cgroup component under the harness subtree.
func (s *Session) Name() string { return s.name }

// Path is the cgroup directory on the cgroup v2 filesystem.
func (s *Session) Path() string { return s.path }

// IsActive reports whether the owning Manager is enabled. The cgroup may not
// exist yet when Create failed.
func (s *Session) IsActive() bool { return s != nil && s.m != nil && s.m.Enabled() }

type sessionKey struct{}

// WithSession binds sess into ctx. run_command reads it back with FromContext
// to place spawned processes in the session cgroup.
func WithSession(ctx context.Context, sess *Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, sess)
}

// FromContext returns the bound session cgroup, or nil.
func FromContext(ctx context.Context) *Session {
	sess, _ := ctx.Value(sessionKey{}).(*Session)
	return sess
}
