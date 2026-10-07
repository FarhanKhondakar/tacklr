package durable

import (
	"testing"

	"github.com/ryanaldo34/tacklr"
)

func TestMemoryEventLog_replaySkipsRetryAndClose(t *testing.T) {
	log := NewMemoryEventLog()
	if head, err := log.Head(t.Context(), "missing"); err != nil || head != 0 {
		t.Fatalf("head %d %v", head, err)
	}
	id := SessionID("s")
	if err := log.Append(t.Context(), id, TopicEvents, tacklr.StreamEvent{Content: "done"}); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(t.Context(), id, TopicRetry, tacklr.StreamEvent{Content: "retry"}); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(t.Context(), id, TopicEvents, tacklr.StreamEvent{Content: "next"}); err != nil {
		t.Fatal(err)
	}
	ch, err := log.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatal(err)
	}
	first, second := <-ch, <-ch
	if first.Content != "done" || second.Content != "next" {
		t.Fatalf("replay %q %q", first.Content, second.Content)
	}
	if err := log.CloseSession(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-ch; ok {
		t.Fatal("subscriber still open")
	}
}
