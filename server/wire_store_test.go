package server_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/server"
)

func TestMemoryWireStore_putGetDelete(t *testing.T) {
	w := server.NewMemoryWireStore()
	if _, err := w.Get(context.Background(), "missing"); !errors.Is(err, server.ErrSessionNotFound) {
		t.Fatalf("missing get: %v", err)
	}
	if err := w.Put(context.Background(), "k", []byte(`{"cwd":"/proj"}`)); err != nil {
		t.Fatal(err)
	}
	got, err := w.Get(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"cwd":"/proj"}` {
		t.Fatalf("get = %s", got)
	}
	if err := w.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Get(context.Background(), "k"); !errors.Is(err, server.ErrSessionNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}
