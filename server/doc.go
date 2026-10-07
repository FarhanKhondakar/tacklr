// Package server serves a durable.Runtime over host-defined wire protocols.
//
// Protocol is the extension point. Implement it to own HTTP routes and WebSocket,
// stream framing, and HITL resume. The ACP built-in is package server/acp.
// More protocols can be mounted on the same Server:
//
//	srv := server.NewServer(rt, cat, acp.New(nil), myProtocol{})
//	_ = srv.ServeHTTP(ctx, addr)
//
// The program on the other end of a connection is the caller's. This module
// does not ship that program. A protocol asks it through Conn.Ask.
//
// RunTurn pumps Runtime.Prompt/Resume/Subscribe through Protocol.OnStreamEvent
// and OnStreamClosed. Map wire credentials into durable.AuthContext on the work
// item. Runtime, harness, VFS, and Temporal do not import this package.
package server
