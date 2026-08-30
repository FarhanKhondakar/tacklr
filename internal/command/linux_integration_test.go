//go:build linux

package command

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/cgroup"
)

// requireWritableSubtree skips the test unless the harness cgroup subtree is
// usable (root, or a user with a delegated subtree). CI runners are non-root
// and skip; local root runs exercise the real kernel path.
func requireWritableSubtree(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err != nil {
		t.Skip("cgroup v2 is not mounted")
	}
	if err := os.MkdirAll(cgroup.DefaultRoot, 0o755); err != nil {
		t.Skipf("harness subtree not writable: %v", err)
	}
}

func TestIntegration_processesAttachedAndKilledOnClose(t *testing.T) {
	requireWritableSubtree(t)
	ctx := context.Background()
	dir := t.TempDir()

	mgr := cgroup.NewManager("")
	sessionID := "int-test-" + strconv.Itoa(os.Getpid())
	sess, err := mgr.Create(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Close(sess) }()

	out, err := Run(cgroup.WithSession(ctx, sess), dir, "sleep 30 & sleep 30 & echo spawned")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "spawned") {
		t.Fatalf("unexpected command output: %q", out)
	}

	// The shell exits; the two background sleeps remain in the session cgroup.
	procs := waitForProcesses(t, sess.Path(), 2)
	if len(procs) == 0 {
		t.Fatal("no processes attached to the session cgroup")
	}
	for _, pid := range procs {
		got, ok := cgroup.SessionIDForPID(pid)
		if !ok || got != sessionID {
			t.Errorf("PID %d resolved to %q, %v; want %q, true", pid, got, ok, sessionID)
		}
	}

	if err := mgr.Close(sess); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, pid := range procs {
		for processAlive(pid) {
			if time.Now().After(deadline) {
				t.Fatalf("PID %d survived cgroup close (cgroup.kill did not reap it)", pid)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if _, err := os.Stat(sess.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("session cgroup dir still present after Close: %v", err)
	}
}

func TestIntegration_reapKillsOrphans(t *testing.T) {
	requireWritableSubtree(t)
	ctx := context.Background()
	dir := t.TempDir()

	mgr := cgroup.NewManager("")
	orphanID := "int-orphan-" + strconv.Itoa(os.Getpid())
	sess, err := mgr.Create(ctx, orphanID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(cgroup.WithSession(ctx, sess), dir, "sleep 30 & echo orphaned"); err != nil {
		t.Fatal(err)
	}
	procs := waitForProcesses(t, sess.Path(), 1)

	mgr.Reap(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for _, pid := range procs {
		for processAlive(pid) {
			if time.Now().After(deadline) {
				t.Fatalf("orphan PID %d survived Reap", pid)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if _, err := os.Stat(sess.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("reaped cgroup dir still present: %v", err)
	}
}

// waitForProcesses polls cgroup.procs until it reports at least want pids.
func waitForProcesses(t *testing.T, cgroupPath string, want int) []int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		procs := readProcesses(t, cgroupPath)
		if len(procs) >= want {
			return procs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d process in %s, have %d", want, cgroupPath, len(procs))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func readProcesses(t *testing.T, cgroupPath string) []int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(cgroupPath, "cgroup.procs"))
	if err != nil {
		return nil
	}
	var pids []int
	for _, line := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(line)
		if err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
