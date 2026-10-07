// Package acp is the Agent Client Protocol built into the server.
//
// JSON-RPC lives here because ACP is JSON-RPC. Another protocol does not
// import this package. The program on the other end of the connection is the
// caller's. This package asks that program for permission, a selection, and
// file credentials. It does not implement that program.
package acp
