// Package command contains the host command execution mechanism. Harness tool
// registration and permission policy remain in the root package.
package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/ryanaldo34/tacklr/internal/cgroup"
)

const outputCap = 1 << 20

// Run executes command in the projected VFS directory and returns a bounded,
// structured stdout/stderr result. Non-zero process exits are successful
// outcomes; startup and context failures are errors. When ctx carries a
// session cgroup (cgroup.WithSession) the shell is attached to that cgroup at
// exec, so the process tree is attributable to the harness session.
func Run(ctx context.Context, dir, command string) (string, error) {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `cd "$1" && eval "$2"`, "run_command", dir, command)
	cmd.Dir = os.TempDir()
	cmd.Stdin = bytes.NewReader(nil)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if attach := sessionCgroupFd(ctx); attach != nil {
		cmd.SysProcAttr.CgroupFD = attach.fd
		cmd.SysProcAttr.UseCgroupFD = true
		defer attach.close()
	}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	budget := outputBudget{remaining: outputCap}
	stdout := budget.writer()
	stderr := budget.writer()
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	exit := 0
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exit = exitError.ExitCode()
		} else {
			return "", fmt.Errorf("run_command: %w", err)
		}
	}
	return formatResult(exit, budget.truncated, stdout, stderr), nil
}

// cgroupFd is an open fd on a session cgroup directory. It is consumed by the
// runtime at fork (CgroupFD) and must stay open until exec, so close() runs
// after Start.
type cgroupFd struct {
	fd    int
	close func()
}

// sessionCgroupFd opens the turn's session cgroup for exec-time attachment. It
// returns nil when ctx has no session cgroup or the directory cannot be
// opened, so the command runs ungrouped rather than failing.
func sessionCgroupFd(ctx context.Context) *cgroupFd {
	sess := cgroup.FromContext(ctx)
	if sess == nil || !sess.IsActive() {
		return nil
	}
	f, err := os.Open(sess.Path())
	if err != nil {
		return nil
	}
	return &cgroupFd{fd: int(f.Fd()), close: func() { _ = f.Close() }}
}

func formatResult(exit int, truncated bool, stdout, stderr *budgetWriter) string {
	var builder strings.Builder
	extra := 0
	if truncated {
		extra = len("output truncated\n")
	}
	builder.Grow(64 + extra + stdout.buf.Len() + stderr.buf.Len())
	fmt.Fprintf(&builder, "exit=%d truncated=%v\n", exit, truncated)
	if truncated {
		builder.WriteString("output truncated\n")
	}
	builder.WriteString("--- stdout ---\n")
	out := stdout.buf.Bytes()
	builder.Write(out)
	if len(out) > 0 && out[len(out)-1] != '\n' {
		builder.WriteByte('\n')
	}
	builder.WriteString("--- stderr ---\n")
	builder.Write(stderr.buf.Bytes())
	return builder.String()
}

type outputBudget struct {
	mu        sync.Mutex
	remaining int
	truncated bool
}

type budgetWriter struct {
	budget *outputBudget
	buf    bytes.Buffer
}

func (b *outputBudget) writer() *budgetWriter {
	return &budgetWriter{budget: b}
}

func (w *budgetWriter) Write(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	w.budget.mu.Lock()
	defer w.budget.mu.Unlock()
	if w.budget.remaining <= 0 {
		w.budget.truncated = true
		return len(value), nil
	}
	take := min(len(value), w.budget.remaining)
	if take < len(value) {
		w.budget.truncated = true
	}
	_, _ = w.buf.Write(value[:take])
	w.budget.remaining -= take
	return len(value), nil
}
