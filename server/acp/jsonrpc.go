package acp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/ryanaldo34/tacklr/server"
)

// JSON-RPC 2.0 error codes. Cancelled follows the ACP / LSP convention.
const (
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInternal       = -32603
	CodeApplication    = -32000
	CodeCancelled      = -32800
)

// ErrorCode maps err to a JSON-RPC 2.0 code.
func ErrorCode(err error) int {
	switch {
	case errors.Is(err, server.ErrMethodNotFound):
		return CodeMethodNotFound
	case errors.Is(err, server.ErrInvalidRequest):
		return CodeInvalidRequest
	case errors.Is(err, context.Canceled):
		return CodeCancelled
	case server.IsClientError(err):
		return CodeApplication
	default:
		return CodeInternal
	}
}

// ErrorBody is one JSON-RPC error envelope.
func ErrorBody(id json.RawMessage, err error) map[string]any {
	pub := server.PublicError(err)
	return map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    ErrorCode(err),
			"message": pub.Error(),
		},
	}
}

func reply(w server.MessageWriter, id json.RawMessage, result any, err error) error {
	if w == nil {
		return err
	}
	if err != nil {
		_ = w.WriteError(id, err)
		return err
	}
	return w.WriteResult(id, result)
}

// HTTPWriter writes JSON-RPC over an HTTP response.
func HTTPWriter(w http.ResponseWriter) server.MessageWriter {
	return &jsonRPCMessageWriter{w: w}
}

type jsonRPCMessageWriter struct {
	w       http.ResponseWriter
	wroteCT bool
}

func (m *jsonRPCMessageWriter) ensureCT() {
	if !m.wroteCT {
		m.w.Header().Set("Content-Type", "application/json")
		m.wroteCT = true
	}
}

func (m *jsonRPCMessageWriter) WriteResult(id json.RawMessage, result any) error {
	m.ensureCT()
	return json.NewEncoder(m.w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (m *jsonRPCMessageWriter) WriteError(id json.RawMessage, err error) error {
	m.ensureCT()
	return json.NewEncoder(m.w).Encode(ErrorBody(id, err))
}

func (m *jsonRPCMessageWriter) WriteFrame(data []byte) error {
	m.ensureCT()
	if _, err := m.w.Write(data); err != nil {
		return err
	}
	_, err := m.w.Write([]byte{'\n'})
	return err
}

type jsonRPCWSMessageWriter struct {
	ctx context.Context
	c   *websocket.Conn
	mu  sync.Mutex
}

func (m *jsonRPCWSMessageWriter) WriteResult(id json.RawMessage, result any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return wsjson.Write(m.ctx, m.c, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (m *jsonRPCWSMessageWriter) WriteError(id json.RawMessage, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return wsjson.Write(m.ctx, m.c, ErrorBody(id, err))
}

func (m *jsonRPCWSMessageWriter) WriteFrame(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c.Write(m.ctx, websocket.MessageText, data)
}
