package inference

import "encoding/json"

// chatRequest is the OpenAI Chat Completions request body (OpenRouter).
type chatRequest struct {
	Model            string             `json:"model"`
	Messages         []chatMessage      `json:"messages"`
	Tools            json.RawMessage    `json:"tools,omitempty"`
	Stream           bool               `json:"stream,omitempty"`
	StreamOptions    *chatStreamOptions `json:"stream_options,omitempty"`
	Reasoning        *chatReasoning     `json:"reasoning,omitempty"`
	IncludeReasoning *bool              `json:"include_reasoning,omitempty"`
	MaxTokens        int                `json:"max_tokens,omitempty"`
}

// chatStreamOptions requests usage on the final streamed chunk.
type chatStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// chatReasoning configures reasoning for effort-based models (ox-series etc.).
type chatReasoning struct {
	Effort string `json:"effort,omitempty"`
}

// chatMessage is one Chat Completions message. Content is plain text for now
// (no multimodal parts).
type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
}

// chatToolCall is an assistant tool call within a message.
type chatToolCall struct {
	ID       string         `json:"id,omitempty"`
	Type     string         `json:"type,omitempty"`
	Function chatToolCallFn `json:"function"`
}

// chatToolCallFn carries the tool name and JSON-encoded arguments.
type chatToolCallFn struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// chatDeltaToolCall is a streamed tool call fragment (delta.tool_calls[i]).
type chatDeltaToolCall struct {
	Index    int                 `json:"index"`
	ID       string              `json:"id,omitempty"`
	Type     string              `json:"type,omitempty"`
	Function chatDeltaToolCallFn `json:"function"`
}

// chatDeltaToolCallFn is the streamed function fragment of a tool call delta.
type chatDeltaToolCallFn struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}
