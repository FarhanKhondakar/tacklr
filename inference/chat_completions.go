package inference

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// OpenRouterInferenceStrategy talks the OpenAI Chat Completions API
// (POST /chat/completions) as served by OpenRouter. It supports streaming,
// tool calling, and reasoning models via include_reasoning + reasoning.effort.
type OpenRouterInferenceStrategy struct {
	instructions string
	apiKey       string
	model        string
	// reasoning effort: "low" | "medium" | "high" | "max" (provider-specific).
	// Sent as reasoning.effort; also enables include_reasoning so thinking
	// content streams back as StreamEventReasoning.
	reasoning string
	// maxOutputTokens is sent as max_tokens when > 0.
	maxOutputTokens  int
	maxContextWindow int
	httpClient       *http.Client
	baseURL          string
}

// defaultOpenRouterContextWindow is used when no explicit window is set.
// Matches the 1M window of stealth/ox-alpha and other long-horizon models.
const defaultOpenRouterContextWindow = 1_000_000

var _ tacklr.InferenceStrategy = (*OpenRouterInferenceStrategy)(nil)

// NewOpenRouterInferenceStrategy builds a strategy for the Chat Completions API.
func NewOpenRouterInferenceStrategy(client *http.Client) *OpenRouterInferenceStrategy {
	if client == nil {
		client = http.DefaultClient
	}
	return &OpenRouterInferenceStrategy{
		httpClient:       client,
		baseURL:          "https://openrouter.ai/api/v1",
		maxContextWindow: defaultOpenRouterContextWindow,
	}
}

func (s *OpenRouterInferenceStrategy) WithApiKey(key string) *OpenRouterInferenceStrategy {
	s.apiKey = key
	return s
}

func (s *OpenRouterInferenceStrategy) WithModel(model string) *OpenRouterInferenceStrategy {
	s.model = model
	return s
}

func (s *OpenRouterInferenceStrategy) WithURL(url string) *OpenRouterInferenceStrategy {
	s.baseURL = url
	return s
}

// WithReasoningLevel sets reasoning.effort and enables include_reasoning so
// provider thinking streams back as StreamEventReasoning.
func (s *OpenRouterInferenceStrategy) WithReasoningLevel(level string) *OpenRouterInferenceStrategy {
	s.reasoning = level
	return s
}

// WithMaxOutputTokens caps completion size via max_tokens (0 omits the field).
func (s *OpenRouterInferenceStrategy) WithMaxOutputTokens(n int) *OpenRouterInferenceStrategy {
	if n < 0 {
		n = 0
	}
	s.maxOutputTokens = n
	return s
}

// WithMaxContextWindow overrides the reported context window (used when the
// harness config does not set MaxWindowSize).
func (s *OpenRouterInferenceStrategy) WithMaxContextWindow(n int) *OpenRouterInferenceStrategy {
	if n <= 0 {
		n = defaultOpenRouterContextWindow
	}
	s.maxContextWindow = n
	return s
}

func (s *OpenRouterInferenceStrategy) SetSystemPrompt(prompt string) {
	s.instructions = prompt
}

// ModelTelemetryIdentity implements the optional harness hook so model spans
// get GenAI provider/model attrs without exporting raw config fields.
func (s *OpenRouterInferenceStrategy) ModelTelemetryIdentity() telemetry.ModelIdentity {
	if s == nil {
		return telemetry.ModelIdentity{}
	}
	return telemetry.NewModelIdentity(s.model, s.baseURL)
}

// SupportsMIME accepts text only. Vision and file input are out of scope for
// the Chat Completions strategy; richer inputs are rejected at the harness.
func (s *OpenRouterInferenceStrategy) SupportsMIME(mimeType string) bool {
	return streaming.IsTextMIME(mimeType)
}

// MaxContextWindow reports the configured context window.
func (s *OpenRouterInferenceStrategy) MaxContextWindow() (int, error) {
	return s.maxContextWindow, nil
}

// CountTokens counts locally with tiktoken. OpenRouter exposes no input token
// counting endpoint, so the count is approximate and provider-agnostic.
func (s *OpenRouterInferenceStrategy) CountTokens(ctx context.Context, messages []*tacklr.Message, tools []*tacklr.Tool) (int, error) {
	if s.apiKey == "" {
		return 0, tacklr.ErrApiKeyNotSet
	}
	if s.model == "" {
		return 0, tacklr.ErrModelNotSet
	}
	tke, err := getEncoding("o200k_base")
	if err != nil {
		return 0, fmt.Errorf("tiktoken count tokens: %w", err)
	}
	var sb strings.Builder
	for _, m := range messages {
		if m == nil {
			continue
		}
		sb.WriteString(m.Content)
		sb.WriteString("\n")
		if m.ToolCallID != "" {
			sb.WriteString(m.ToolCallID)
			sb.WriteString("\n")
		}
		for _, tc := range m.ToolCalls {
			sb.WriteString(tc.Name)
			sb.WriteString("\n")
			sb.WriteString(tc.Arguments)
			sb.WriteString("\n")
		}
	}
	if len(tools) > 0 {
		sb.WriteString(tacklr.ToolsAsJson(tools))
	}
	return len(tke.Encode(sb.String(), nil, nil)), nil
}

// Invoke streams a Chat Completions turn from messages + tools.
func (s *OpenRouterInferenceStrategy) Invoke(ctx context.Context, messages []*tacklr.Message, tools []*tacklr.Tool, systemPrompt string) (chan tacklr.LLMResponseChunk, error) {
	if s.apiKey == "" {
		return nil, tacklr.ErrApiKeyNotSet
	}
	if s.model == "" {
		return nil, tacklr.ErrModelNotSet
	}

	chatMsgs := marshalMessagesToChat(messages)
	reqBody := chatRequest{
		Model:         s.model,
		Messages:      chatMsgs,
		Stream:        true,
		StreamOptions: &chatStreamOptions{IncludeUsage: true},
	}
	if len(tools) > 0 {
		reqBody.Tools = chatToolsJSON(tools)
	}
	if s.maxOutputTokens > 0 {
		reqBody.MaxTokens = s.maxOutputTokens
	}
	if s.reasoning != "" {
		reqBody.Reasoning = &chatReasoning{Effort: s.reasoning}
		inc := true
		reqBody.IncludeReasoning = &inc
	}

	prompt := systemPrompt
	if prompt == "" {
		prompt = s.instructions
	}
	if prompt != "" {
		reqBody.Messages = append([]chatMessage{{Role: "system", Content: prompt}}, reqBody.Messages...)
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal model request: %w", err)
	}
	inputSummary := summarizeChatMessages(chatMsgs)

	events := make(chan tacklr.LLMResponseChunk, 10)

	sendChunk := func(chunk tacklr.LLMResponseChunk) {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
		case events <- chunk:
		}
	}
	sendErr := func(err error) {
		if err == nil {
			return
		}
		sendChunk(tacklr.LLMResponseChunk{
			Type:       tacklr.StreamEventError,
			Content:    err.Error(),
			Error:      err,
			IsComplete: true,
		})
	}

	go func() {
		defer close(events)

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.baseURL, "/")+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			err = fmt.Errorf("build provider request: %w", err)
			slog.ErrorContext(ctx, "failed to build model provider request", "error", err)
			sendErr(err)
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+s.apiKey)

		httpResp, err := s.httpClient.Do(httpReq)
		if err != nil {
			err = fmt.Errorf("model provider request failed: %w", err)
			slog.ErrorContext(ctx, "model provider request failed", "error", err)
			sendErr(err)
			return
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode != http.StatusOK {
			respBody, readErr := io.ReadAll(httpResp.Body)
			if readErr != nil {
				slog.WarnContext(ctx, "could not read provider error body", "error", readErr, "http_status", httpResp.StatusCode)
			}
			classified := classifyProviderFailure(httpResp.StatusCode, respBody)
			emitProviderFailed(ctx, classified, httpResp.StatusCode, inputSummary, string(respBody))
			sendErr(classified)
			return
		}

		s.parseChatSSE(ctx, httpResp.Body, events, inputSummary)
	}()

	return events, nil
}

// parseChatSSE maps OpenAI Chat Completions streaming events to chunks.
// Content deltas stream as messages, reasoning as reasoning, accumulated tool
// call deltas flush as a single function_call chunk at the terminal event, and
// usage arrives on the final chunk via stream_options.include_usage.
func (s *OpenRouterInferenceStrategy) parseChatSSE(ctx context.Context, body io.Reader, events chan<- tacklr.LLMResponseChunk, inputSummary string) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	terminal := false
	sawToolCall := false
	var toolCalls []chatDeltaToolCall

	emitFailure := func(err error, detail string) {
		emitProviderFailed(ctx, err, http.StatusOK, inputSummary, detail)
		events <- tacklr.LLMResponseChunk{
			Type:       tacklr.StreamEventError,
			Content:    err.Error(),
			Error:      err,
			IsComplete: true,
		}
	}

	for scanner.Scan() {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		line := scanner.Text()

		const prefix = "data: "
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		data := line[len(prefix):]
		if data == "[DONE]" {
			if !terminal {
				emitFailure(ErrIncompleteStream, "provider sent [DONE] before a terminal finish_reason")
			}
			return
		}

		var chunk struct {
			Choices []struct {
				Index int `json:"index"`
				Delta struct {
					Role             string              `json:"role"`
					Content          string              `json:"content"`
					Reasoning        string              `json:"reasoning"`
					ReasoningDetails json.RawMessage     `json:"reasoning_details"`
					ToolCalls        []chatDeltaToolCall `json:"tool_calls"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens            int `json:"prompt_tokens"`
				CompletionTokens        int `json:"completion_tokens"`
				CompletionTokensDetails *struct {
					ReasoningTokens int `json:"reasoning_tokens"`
				} `json:"completion_tokens_details"`
			} `json:"usage"`
			Error *apiErrorDetail `json:"error"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			emitFailure(fmt.Errorf("%w: %w", ErrMalformedStream, err), "")
			return
		}
		if chunk.Error != nil {
			classified := classifyAPIStatus(&APIStatusError{Status: 200, Body: chunk.Error.Message, Code: chunk.Error.Code}, chunk.Error.Type)
			emitProviderFailed(ctx, classified, 200, inputSummary, data)
			events <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventError,
				Content:    classified.Error(),
				Error:      classified,
				IsComplete: true,
			}
			return
		}
		if len(chunk.Choices) == 0 {
			if chunk.Usage != nil && terminal {
				events <- chatUsageChunk(chunk.Usage)
			}
			continue
		}

		choice := chunk.Choices[0]
		if text := chatReasoningText(choice.Delta); text != "" {
			events <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventReasoning,
				Content:    text,
				IsComplete: false,
			}
		}
		if choice.Delta.Content != "" {
			events <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventMessage,
				Content:    choice.Delta.Content,
				IsComplete: false,
			}
		}
		for _, tc := range choice.Delta.ToolCalls {
			toolCalls = mergeChatToolCall(toolCalls, tc)
			sawToolCall = true
		}

		if choice.FinishReason == "" {
			continue
		}
		terminal = true

		if sawToolCall {
			events <- chatFunctionCallChunk(toolCalls)
		}

		switch choice.FinishReason {
		case "length":
			classified := tacklr.WrapStopReason(tacklr.ErrMaxTokens, fmt.Errorf("finish_reason=length"))
			emitProviderFailed(ctx, classified, 200, inputSummary, data)
			events <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventError,
				Content:    classified.Error(),
				Error:      classified,
				IsComplete: true,
			}
			return
		case "content_filter":
			classified := tacklr.WrapStopReason(tacklr.ErrModelRefused, fmt.Errorf("finish_reason=content_filter"))
			emitProviderFailed(ctx, classified, 200, inputSummary, data)
			events <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventError,
				Content:    classified.Error(),
				Error:      classified,
				IsComplete: true,
			}
			return
		default:
			if chunk.Usage != nil {
				events <- chatUsageChunk(chunk.Usage)
			}
		}
	}
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if err := scanner.Err(); err != nil {
		emitFailure(fmt.Errorf("%w: %w", ErrIncompleteStream, err), "")
		return
	}
	if !terminal {
		emitFailure(ErrIncompleteStream, "provider stream closed before a terminal finish_reason")
	}
}

// chatUsageChunk surfaces provider token usage as a complete chunk (the harness
// reads InputTokens/OutputTokens/ReasoningTokens from any chunk it sees).
func chatUsageChunk(u *struct {
	PromptTokens            int `json:"prompt_tokens"`
	CompletionTokens        int `json:"completion_tokens"`
	CompletionTokensDetails *struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}) tacklr.LLMResponseChunk {
	chunk := tacklr.LLMResponseChunk{
		Type:         tacklr.StreamEventComplete,
		IsComplete:   true,
		InputTokens:  u.PromptTokens,
		OutputTokens: u.CompletionTokens,
	}
	if u.CompletionTokensDetails != nil {
		chunk.ReasoningTokens = u.CompletionTokensDetails.ReasoningTokens
	}
	return chunk
}

// chatFunctionCallChunk flushes accumulated tool call deltas as a completed
// function_call chunk. ID and CallID both carry the provider id so the harness
// tool result pairing resolves via WireID.
func chatFunctionCallChunk(calls []chatDeltaToolCall) tacklr.LLMResponseChunk {
	tcs := make([]tacklr.ToolCall, 0, len(calls))
	for _, c := range calls {
		name := c.Function.Name
		args := c.Function.Arguments
		if args == "" {
			args = "{}"
		}
		tcs = append(tcs, tacklr.ToolCall{
			ID:        c.ID,
			CallID:    c.ID,
			Type:      "function_call",
			Name:      name,
			Arguments: args,
			Status:    string(tacklr.StatusCompleted),
		})
	}
	return tacklr.LLMResponseChunk{
		Type:       tacklr.StreamEventFunctionCall,
		ToolCalls:  tcs,
		IsComplete: true,
	}
}

// mergeChatToolCall folds a streamed tool call delta into the accumulated list
// keyed by index. OpenAI streams id/name once and arguments across fragments.
func mergeChatToolCall(calls []chatDeltaToolCall, delta chatDeltaToolCall) []chatDeltaToolCall {
	for i := range calls {
		if calls[i].Index != delta.Index {
			continue
		}
		if delta.ID != "" {
			calls[i].ID = delta.ID
		}
		if delta.Function.Name != "" {
			calls[i].Function.Name = delta.Function.Name
		}
		if delta.Function.Arguments != "" {
			calls[i].Function.Arguments += delta.Function.Arguments
		}
		return calls
	}
	return append(calls, delta)
}

// chatReasoningText extracts streaming reasoning from delta.reasoning or
// delta.reasoning_details (OpenRouter's Anthropic-style shape).
func chatReasoningText(delta struct {
	Role             string              `json:"role"`
	Content          string              `json:"content"`
	Reasoning        string              `json:"reasoning"`
	ReasoningDetails json.RawMessage     `json:"reasoning_details"`
	ToolCalls        []chatDeltaToolCall `json:"tool_calls"`
}) string {
	if delta.Reasoning != "" {
		return delta.Reasoning
	}
	if len(delta.ReasoningDetails) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(delta.ReasoningDetails, &s) == nil {
		return s
	}
	var parts []struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(delta.ReasoningDetails, &parts) == nil {
		var b strings.Builder
		for _, p := range parts {
			b.WriteString(p.Text)
		}
		return b.String()
	}
	return ""
}

// marshalMessagesToChat converts the harness window into Chat Completions
// messages. Reasoning history is dropped (the model re-reasons each turn).
// Unmatched tool outputs are dropped to avoid invalid tool_call_id payloads.
func marshalMessagesToChat(messages []*tacklr.Message) []chatMessage {
	var out []chatMessage
	var callIDs map[string]struct{}

	for _, msg := range messages {
		if msg == nil {
			continue
		}
		switch msg.Role {
		case tacklr.RoleTool:
			id := strings.TrimSpace(msg.ToolCallID)
			if id == "" {
				continue
			}
			if _, ok := callIDs[id]; !ok {
				continue
			}
			out = append(out, chatMessage{Role: "tool", ToolCallID: id, Content: msg.Content})
		case tacklr.RoleUser, tacklr.RoleSystem, tacklr.RoleDeveloper:
			role := string(msg.Role)
			if msg.Role == tacklr.RoleDeveloper {
				role = "system"
			}
			out = append(out, chatMessage{Role: role, Content: msg.Content})
			callIDs = nil
		case tacklr.RoleAssistant:
			callIDs = make(map[string]struct{})
			if msg.Content == "" && len(msg.ToolCalls) == 0 {
				continue
			}
			cm := chatMessage{Role: "assistant", Content: msg.Content}
			for _, tc := range msg.ToolCalls {
				id := tc.WireID()
				if id == "" {
					continue
				}
				callIDs[id] = struct{}{}
				name := tc.Name
				if tc.Namespace != "" && name != "" && !strings.Contains(name, ".") {
					name = tc.Namespace + "." + name
				}
				args := tc.Arguments
				if args == "" {
					args = "{}"
				}
				cm.ToolCalls = append(cm.ToolCalls, chatToolCall{
					ID:   id,
					Type: "function",
					Function: chatToolCallFn{
						Name:      name,
						Arguments: args,
					},
				})
			}
			out = append(out, cm)
		case tacklr.RoleReasoning:
			// dropped
		}
	}
	return out
}

// chatToolsJSON wraps flat Responses-style tool defs in the Chat Completions
// {type:function, function:{...}} shape. Only name, description, and parameters
// are forwarded; strict/type are Responses-API concepts the chat endpoint drops.
func chatToolsJSON(tools []*tacklr.Tool) json.RawMessage {
	defs := make([]map[string]any, 0, len(tools))
	for _, t := range tools {
		flat := t.AsJson()
		defs = append(defs, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        flat["name"],
				"description": flat["description"],
				"parameters":  flat["parameters"],
			},
		})
	}
	b, _ := json.Marshal(defs)
	return b
}

// summarizeChatMessages is a short, safe log line of request input shape.
func summarizeChatMessages(msgs []chatMessage) string {
	if len(msgs) == 0 {
		return "empty"
	}
	parts := make([]string, 0, len(msgs))
	for i, m := range msgs {
		extra := ""
		if m.ToolCallID != "" {
			extra = " call_id=" + m.ToolCallID
		}
		if len(m.ToolCalls) > 0 {
			extra = fmt.Sprintf(" tool_calls=%d", len(m.ToolCalls))
		}
		parts = append(parts, fmt.Sprintf("%d:%s%s", i, m.Role, extra))
		if i >= 24 {
			parts = append(parts, fmt.Sprintf("…+%d more", len(msgs)-i-1))
			break
		}
	}
	return strings.Join(parts, "; ")
}
