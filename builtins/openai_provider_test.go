package builtins

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

// TestInvoke_sendsProviderRouting proves the OpenRouter provider preference
// reaches the wire: the request body carries provider.only and
// allow_fallbacks. Without it a pinned model can be served by another backend.
func TestInvoke_sendsProviderRouting(t *testing.T) {
	var sawBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			http.NotFound(w, r)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		if err := json.Unmarshal(raw, &sawBody); err != nil {
			t.Errorf("unmarshal body: %v", err)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenRouterInferenceStrategy(srv.Client()).
		WithApiKey("test-key").
		WithModel("inclusionai/ling-3.0-flash").
		WithURL(srv.URL).
		WithProviderRouting([]string{"novita"}, false)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	for range ch {
	}

	provider, ok := sawBody["provider"].(map[string]any)
	if !ok {
		t.Fatalf("provider missing from body: %v", sawBody["provider"])
	}
	only, _ := provider["only"].([]any)
	if len(only) != 1 || only[0] != "novita" {
		t.Errorf("provider.only = %v, want [novita]", provider["only"])
	}
	if provider["allow_fallbacks"] != false {
		t.Errorf("provider.allow_fallbacks = %v, want false", provider["allow_fallbacks"])
	}
}
