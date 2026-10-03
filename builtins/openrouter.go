package builtins

import "net/http"

// openRouterBaseURL is OpenRouter's OpenAI-compatible base URL. The strategy
// appends the Responses API paths (/responses, /responses/input_tokens) to it.
const openRouterBaseURL = "https://openrouter.ai/api/v1"

// NewOpenRouterInferenceStrategy returns an OpenAI Responses strategy
// preconfigured for OpenRouter.
//
// OpenRouter validates prompt_cache_options.mode and rejects "implicit", so the
// strategy pins a key-only cache profile: prompt_cache_key is sent, while the
// GPT-5.6 cache options and content-block breakpoints stay off the wire.
func NewOpenRouterInferenceStrategy(client *http.Client) *OpenAIInferenceStrategy {
	s := NewOpenAIInferenceStrategy(client).WithURL(openRouterBaseURL)
	s.promptCacheFor = func(_, _, key string) promptCache {
		return openRouterCache{key: key}
	}
	return s
}

// openRouterCache sends prompt_cache_key only. OpenRouter rejects the GPT-5.6
// prompt_cache_options payload, and breakpoints are part of that same policy.
type openRouterCache struct{ key string }

func (c openRouterCache) apply(req *responsesRequest) { req.PromptCacheKey = c.key }
func (openRouterCache) headers(http.Header)           {}
func (openRouterCache) breakpoints() bool             { return false }
