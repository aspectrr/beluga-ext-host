package ext_host

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	sdk "github.com/aspectrr/beluga-ext-sdk/belugav1"
	"github.com/collinpfeifer/beluga/pkg/tools"
)

// RemoteExtServer implements the ExtensionHostService gRPC service.
// It handles external processes connecting to Beluga, registering their
// tools, and proxying tool execution requests.
type RemoteExtServer struct {
	sdk.UnimplementedExtensionHostServiceServer
	provider *GRPCProvider
	registry *tools.Registry
	logger   *slog.Logger

	mu          sync.RWMutex
	connections map[string]*remoteConnection // extension name → connection
}

// remoteConnection tracks a connected remote extension.
type remoteConnection struct {
	name    string
	stream  sdk.ExtensionHostService_ConnectServer
	tools   []string                // tool names registered by this extension
	pending map[string]chan *sdk.ToolResult // callID → result channel
	mu      sync.Mutex
}

// NewRemoteExtServer creates a new remote extension host server.
func NewRemoteExtServer(provider *GRPCProvider, registry *tools.Registry, logger *slog.Logger) *RemoteExtServer {
	return &RemoteExtServer{
		provider:    provider,
		registry:    registry,
		logger:      logger,
		connections: make(map[string]*remoteConnection),
	}
}

// Connect handles the bidirectional stream from a remote extension process.
// The remote sends a Registration message first (with tool definitions),
// then receives ExecuteTool requests and returns ToolResult responses.
func (s *RemoteExtServer) Connect(stream sdk.ExtensionHostService_ConnectServer) error {
	var conn *remoteConnection

	defer func() {
		if conn != nil {
			s.unregisterConnection(conn)
		}
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch payload := msg.Payload.(type) {
		case *sdk.ExtensionMessage_Registration:
			reg := payload.Registration
			conn = &remoteConnection{
				name:    reg.ExtensionName,
				stream:  stream,
				tools:   make([]string, 0, len(reg.Tools)),
				pending: make(map[string]chan *sdk.ToolResult),
			}

			// Register each tool from the remote extension.
			for _, td := range reg.Tools {
				remoteTool := &remoteTool{
					name:        td.Name,
					description: td.Description,
					parameters:  json.RawMessage(td.Parameters),
					conn:        conn,
				}
				if err := s.registry.Register(remoteTool); err != nil {
					s.logger.Warn("failed to register remote tool",
						"extension", reg.ExtensionName,
						"tool", td.Name,
						"error", err,
					)
					continue
				}
				conn.tools = append(conn.tools, td.Name)
			}

			s.registerConnection(conn)
			s.logger.Info("remote extension connected",
				"extension", reg.ExtensionName,
				"tools", len(conn.tools),
			)

		case *sdk.ExtensionMessage_ToolResult:
			if conn == nil {
				continue
			}
			conn.mu.Lock()
			ch, ok := conn.pending[payload.ToolResult.CallId]
			if ok {
				delete(conn.pending, payload.ToolResult.CallId)
			}
			conn.mu.Unlock()

			if ok {
				ch <- payload.ToolResult
			}

		default:
			s.logger.Warn("unknown message from remote extension")
		}
	}
}

// registerConnection adds a remote connection to the tracked connections.
func (s *RemoteExtServer) registerConnection(conn *remoteConnection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If this extension was already connected, clean up the old one.
	if old, exists := s.connections[conn.name]; exists {
		s.logger.Warn("replacing existing remote extension connection",
			"extension", conn.name,
		)
		// Remove old tools from registry.
		for _, toolName := range old.tools {
			s.registry.Unregister(toolName)
		}
	}

	s.connections[conn.name] = conn
}

// unregisterConnection removes a remote connection and its tools.
func (s *RemoteExtServer) unregisterConnection(conn *remoteConnection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.connections[conn.name]; exists {
		delete(s.connections, conn.name)
	}

	// Remove tools from registry.
	for _, toolName := range conn.tools {
		s.registry.Unregister(toolName)
	}

	s.logger.Info("remote extension disconnected",
		"extension", conn.name,
		"tools_removed", len(conn.tools),
	)
}

// sendToolCall sends an ExecuteTool request to the remote extension
// and waits for the result. Used by remoteTool.Execute().
func (s *RemoteExtServer) sendToolCall(ctx context.Context, conn *remoteConnection, callID, toolName string, args json.RawMessage) (json.RawMessage, error) {
	// Register pending result channel.
	resultCh := make(chan *sdk.ToolResult, 1)
	conn.mu.Lock()
	conn.pending[callID] = resultCh
	conn.mu.Unlock()

	defer func() {
		conn.mu.Lock()
		delete(conn.pending, callID)
		conn.mu.Unlock()
	}()

	// Send the tool call request.
	if err := conn.stream.Send(&sdk.HostRequest{
		Payload: &sdk.HostRequest_ExecuteTool{
			ExecuteTool: &sdk.ExecuteTool{
				CallId:    callID,
				ToolName:  toolName,
				Arguments: string(args),
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("sending tool call to remote extension: %w", err)
	}

	// Wait for result or context cancellation.
	select {
	case result := <-resultCh:
		if result.IsError {
			return nil, fmt.Errorf("remote tool error: %s", result.Output)
		}
		return json.RawMessage(result.Output), nil
	case <-ctx.Done():
		return nil, fmt.Errorf("remote tool call timed out: %w", ctx.Err())
	}
}

// remoteTool implements the tools.Tool interface for tools provided
// by remote extensions. Execution is proxied over gRPC.
type remoteTool struct {
	name        string
	description string
	parameters  json.RawMessage
	conn        *remoteConnection
}

func (t *remoteTool) Definition() tools.ToolDef {
	return tools.ToolDef{
		Name:        t.name,
		Description: t.description,
		Parameters:  t.parameters,
	}
}

func (t *remoteTool) Execute(ctx context.Context, args json.RawMessage, tctx tools.ToolContext) (json.RawMessage, error) {
	// Generate a call ID from session ID + tool name for tracking.
	callID := fmt.Sprintf("%s-%s", tctx.SessionID, t.name)

	resultCh := make(chan *sdk.ToolResult, 1)
	t.conn.mu.Lock()
	t.conn.pending[callID] = resultCh
	t.conn.mu.Unlock()

	defer func() {
		t.conn.mu.Lock()
		delete(t.conn.pending, callID)
		t.conn.mu.Unlock()
	}()

	if err := t.conn.stream.Send(&sdk.HostRequest{
		Payload: &sdk.HostRequest_ExecuteTool{
			ExecuteTool: &sdk.ExecuteTool{
				CallId:    callID,
				ToolName:  t.name,
				Arguments: string(args),
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("sending tool call to remote extension %q: %w", t.conn.name, err)
	}

	select {
	case result := <-resultCh:
		if result.IsError {
			return nil, fmt.Errorf("remote tool %q error: %s", t.name, result.Output)
		}
		return json.RawMessage(result.Output), nil
	case <-ctx.Done():
		return nil, fmt.Errorf("remote tool %q call timed out: %w", t.name, ctx.Err())
	}
}
