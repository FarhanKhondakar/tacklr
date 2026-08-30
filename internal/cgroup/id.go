// Package cgroup manages cgroup v2 session isolation for harness sessions.
// Each session owns one cgroup under a delegated subtree (default
// /sys/fs/cgroup/harness). Processes the session spawns are attached at exec
// through SysProcAttr.CgroupFD, so child processes inherit the cgroup and a
// host security monitor can attribute any PID back to its session.
package cgroup

import (
	"fmt"
	"os"
	"strings"
)

// DefaultRootName is the harness subtree component on the cgroup v2
// filesystem. Session cgroups live at /sys/fs/cgroup/harness/<encoded>.
// Security monitors match this segment in /proc/<pid>/cgroup and decode the
// next segment back to the harness SessionID.
const DefaultRootName = "harness"

// EncodeID maps a harness SessionID to a cgroup v2 directory component. The
// mapping is deterministic and reversible: safe bytes are kept and everything
// else (including '_') is escaped as _<hex> so the result only contains
// [A-Za-z0-9_.-], the cgroup name alphabet.
func EncodeID(id string) string {
	if id == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(id))
	for i := 0; i < len(id); i++ {
		c := id[i]
		if isSafe(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('_')
		b.WriteByte(lowerHex(c >> 4))
		b.WriteByte(lowerHex(c & 0x0f))
	}
	return b.String()
}

// DecodeID reverses EncodeID. It reports false for malformed input.
func DecodeID(encoded string) (string, bool) {
	if encoded == "" {
		return "", true
	}
	var b strings.Builder
	b.Grow(len(encoded))
	for i := 0; i < len(encoded); i++ {
		c := encoded[i]
		if c == '_' {
			if i+2 >= len(encoded) {
				return "", false
			}
			hi, okHi := hexVal(encoded[i+1])
			lo, okLo := hexVal(encoded[i+2])
			if !okHi || !okLo {
				return "", false
			}
			b.WriteByte(hi<<4 | lo)
			i += 2
			continue
		}
		if !isSafe(c) {
			return "", false
		}
		b.WriteByte(c)
	}
	return b.String(), true
}

func isSafe(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '.'
}

func lowerHex(n byte) byte { return "0123456789abcdef"[n] }

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	default:
		return 0, false
	}
}

// SessionIDForPID resolves the harness session owning pid by reading
// /proc/<pid>/cgroup and decoding the DefaultRootName segment. ok is false
// when pid is unreadable or not inside a harness session cgroup.
func SessionIDForPID(pid int) (string, bool) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", false
	}
	return SessionIDForProcContent(string(data))
}

// SessionIDForProcContent extracts the harness session id from a raw
// /proc/<pid>/cgroup body. ok is false when no DefaultRootName segment matches.
func SessionIDForProcContent(content string) (string, bool) {
	for _, line := range strings.Split(content, "\n") {
		if id, ok := SessionIDForCgroupLine(line); ok {
			return id, true
		}
	}
	return "", false
}

// SessionIDForCgroupLine parses one /proc/<pid>/cgroup line. The path is the
// last ':'-separated field (v2: "0::/path", v1: "hierarchy:controllers:/path").
func SessionIDForCgroupLine(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", false
	}
	_, rest, ok := strings.Cut(line, ":")
	if !ok {
		return "", false
	}
	_, path, ok := strings.Cut(rest, ":")
	if !ok {
		return "", false
	}
	return sessionIDFromPath(path)
}

func sessionIDFromPath(path string) (string, bool) {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	for i, seg := range segs {
		if seg != DefaultRootName || i+1 >= len(segs) {
			continue
		}
		return DecodeID(segs[i+1])
	}
	return "", false
}
