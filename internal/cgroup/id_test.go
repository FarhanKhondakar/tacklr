package cgroup

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestEncodeDecodeID_roundTrip(t *testing.T) {
	cases := []string{
		"",
		"7f8c2a1e-3b4d-4e5f-8a9b-0c1d2e3f4a5b",
		"parent/w/researcher/c1",
		"a parent/w/worker: with spaces!",
		"dots.and-dashes_ok",
		"UPPER_lower_123",
		"ünïcode/工作",
		"parent/w/specialist name/_odd",
	}
	for _, id := range cases {
		enc := EncodeID(id)
		if enc != "" && !safeName(enc) {
			t.Errorf("EncodeID(%q) = %q, contains chars outside the cgroup name alphabet", id, enc)
		}
		got, ok := DecodeID(enc)
		if !ok || got != id {
			t.Errorf("DecodeID(EncodeID(%q)) = %q, %v; want %q, true", id, got, ok, id)
		}
	}
}

func TestDecodeID_malformed(t *testing.T) {
	for _, enc := range []string{"_", "_2", "_2g", "_gg", "a_b", "good?bad", "has/ slash"} {
		if _, ok := DecodeID(enc); ok {
			t.Errorf("DecodeID(%q) unexpectedly ok", enc)
		}
	}
}

func TestDecodeID_emptyIsValid(t *testing.T) {
	if got, ok := DecodeID(""); !ok || got != "" {
		t.Errorf("DecodeID(\"\") = %q, %v; want \"\", true", got, ok)
	}
}

func safeName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func TestSessionIDForCgroupLine(t *testing.T) {
	id := "parent/w/researcher/c1"
	enc := EncodeID(id)
	cases := []struct {
		line string
		want string
		ok   bool
	}{
		{fmt.Sprintf("0::/harness/%s", enc), id, true},
		{fmt.Sprintf("0::/system.slice/unit.service/harness/%s", enc), id, true},
		{fmt.Sprintf("2:cpu:/harness/%s", enc), id, true},
		{"0::/system.slice/unit.service", "", false},
		{"0::/harness", "", false},
		{"", "", false},
		{"0::/", "", false},
		{"not a cgroup line", "", false},
		{"0:single-colon-only", "", false},
		{"0::/other/%s", "", false},
	}
	for _, tc := range cases {
		got, ok := SessionIDForCgroupLine(tc.line)
		if got != tc.want || ok != tc.ok {
			t.Errorf("SessionIDForCgroupLine(%q) = %q, %v; want %q, %v", tc.line, got, ok, tc.want, tc.ok)
		}
	}
}

func TestSessionIDForProcContent(t *testing.T) {
	id := "sess-1"
	enc := EncodeID(id)
	body := "0::/system.slice/a.service\n0::/system.slice/a.service/harness/" + enc + "\n"
	got, ok := SessionIDForProcContent(body)
	if !ok || got != id {
		t.Errorf("SessionIDForProcContent = %q, %v; want %q, true", got, ok, id)
	}
	if _, ok := SessionIDForProcContent("0::/system.slice\n0::/\n"); ok {
		t.Error("SessionIDForProcContent matched a non-harness body")
	}
}

func TestSessionIDForPID_readsProcCgroup(t *testing.T) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	want, wantOK := SessionIDForProcContent(string(data))
	got, ok := SessionIDForPID(os.Getpid())
	if got != want || ok != wantOK {
		t.Errorf("SessionIDForPID(own pid) = %q, %v; parser says %q, %v", got, ok, want, wantOK)
	}
	if _, ok := SessionIDForPID(-1); ok {
		t.Error("SessionIDForPID(-1) matched")
	}
}

func TestEncodeID_escapeAmbiguity(t *testing.T) {
	id := "_2f"
	enc := EncodeID(id)
	if enc != "_5f2f" {
		t.Errorf("EncodeID(%q) = %q; want _5f2f (literal underscore escaped)", id, enc)
	}
	got, ok := DecodeID(enc)
	if !ok || got != id {
		t.Errorf("DecodeID(_5f2f) = %q, %v; want %q, true", got, ok, id)
	}
	if strings.Contains(enc, "_2f") {
		t.Errorf("escape sequence appears as a substring of %q", enc)
	}
}
