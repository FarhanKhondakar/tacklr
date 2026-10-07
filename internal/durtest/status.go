package durtest

import (
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
)

// AssertStatusMatchesEvent checks Runtime.Status against a turn-finished stream
// event. complete → SessionComplete, error → SessionFailed, yield → running+Waiting.
func AssertStatusMatchesEvent(t testing.TB, rt session.Runtime, id session.SessionID, ev tacklr.StreamEvent) {
	t.Helper()
	switch ev.Type {
	case tacklr.StreamEventComplete, tacklr.StreamEventError, tacklr.StreamEventInterrupt:
	default:
		return
	}
	st, err := rt.Status(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	switch ev.Type {
	case tacklr.StreamEventComplete:
		if st.State != session.SessionComplete {
			t.Fatalf("complete event but Status %+v", st)
		}
	case tacklr.StreamEventError:
		if st.State != session.SessionFailed {
			t.Fatalf("error event but Status %+v", st)
		}
	case tacklr.StreamEventInterrupt:
		if st.State != session.SessionRunning || !st.Waiting {
			t.Fatalf("yield event but Status %+v", st)
		}
	}
}
