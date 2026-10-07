package durable

import (
	"errors"
	"sync"
	"testing"
)

func TestMemorySnapshot_revisionRejectsStaleSave(t *testing.T) {
	store := NewMemorySnapshot()
	id := SessionID("s")
	rev, err := store.Save(t.Context(), id, Snapshot{Specialist: "a"}, "")
	if err != nil || rev == "" {
		t.Fatal(err)
	}
	if _, err := store.Save(t.Context(), id, Snapshot{Specialist: "b"}, ""); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("stale: %v", err)
	}
	rev2, err := store.Save(t.Context(), id, Snapshot{Specialist: "b"}, rev)
	if err != nil {
		t.Fatal(err)
	}
	snap, got, err := store.Load(t.Context(), id)
	if err != nil || snap.Specialist != "b" || got != rev2 {
		t.Fatalf("load %+v rev %q err %v", snap, got, err)
	}
	if err := store.Delete(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(t.Context(), id); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("deleted: %v", err)
	}
}

func TestMemorySnapshot_concurrentSaveOneWins(t *testing.T) {
	store := NewMemorySnapshot()
	id := SessionID("s")
	rev, err := store.Save(t.Context(), id, Snapshot{Specialist: "base"}, "")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var wins, stale int
	for _, name := range []string{"left", "right"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Save(t.Context(), id, Snapshot{Specialist: name}, rev)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins++
				return
			}
			if errors.Is(err, ErrStaleCheckpoint) {
				stale++
			}
		}()
	}
	wg.Wait()
	if wins != 1 || stale != 1 {
		t.Fatalf("wins=%d stale=%d", wins, stale)
	}
	snap, _, err := store.Load(t.Context(), id)
	if err != nil || (snap.Specialist != "left" && snap.Specialist != "right") {
		t.Fatalf("winner %+v err %v", snap, err)
	}
}
