package tacklr

import "testing"

// TestHookOutputOverride proves a host ToolResultHook can replace the
// model-visible tool output, which researchr uses to cap worker results.
func TestHookOutputOverride(t *testing.T) {
	if got := hookOutput("raw output", ToolOutcome{Output: "capped"}); got != "capped" {
		t.Fatalf("hookOutput = %q, want capped", got)
	}
	if got := hookOutput("raw output", ToolOutcome{}); got != "raw output" {
		t.Fatalf("hookOutput = %q, want the raw output when the hook stays silent", got)
	}
}
