package session

import (
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestCredentialBag_bindRefreshUnbindReachesTheNextPrompt(t *testing.T) {
	// Arrange
	var bag CredentialBag
	expires := time.Now().Add(time.Hour).UTC()
	first := vfs.Binding{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "tok1"},
	}
	if err := bag.Bind(first); err != nil {
		t.Fatal(err)
	}
	if err := bag.Bind(vfs.Binding{
		Provider: "local",
		Point:    "/docs",
		Auth:     vfs.Credential{Token: "tok1b", ExpiresAt: expires},
	}); err != nil {
		t.Fatal(err)
	}

	// Act
	got := bag.Take()

	// Assert
	if len(got.Bindings) != 1 || got.Bindings[0].Auth.Token != "tok1b" || !got.Bindings[0].Auth.ExpiresAt.Equal(expires) {
		t.Fatalf("replaced binding = %+v", got.Bindings)
	}
	if !bag.Refresh("local", vfs.Credential{Token: "tok2"}) {
		t.Fatal("refresh missed the local binding")
	}
	if bag.Refresh("ghost", vfs.Credential{Token: "x"}) {
		t.Fatal("refresh reported a provider that was never bound")
	}
	refreshed := bag.Take()
	if len(refreshed.Bindings) != 1 || refreshed.Bindings[0].Auth.Token != "tok2" || len(refreshed.Drop) != 0 {
		t.Fatalf("after refresh = %+v", refreshed)
	}
	bag.Unbind("/docs", "")
	cleared := bag.Take()
	if len(cleared.Bindings) != 0 || len(cleared.Drop) != 1 || cleared.Drop[0] != "docs" {
		t.Fatalf("after unbind = %+v", cleared)
	}
}
