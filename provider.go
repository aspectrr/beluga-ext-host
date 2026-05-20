package ext_host

import (
	"log/slog"
	"net"
	"sync"

	"google.golang.org/grpc"
)

// GRPCProvider provides the shared gRPC server and connection management.
// The ext_host extension creates this and sets it on ExtensionContext.GRPC
// so other extensions (like remora) can register services on it.
type GRPCProvider struct {
	server  *grpc.Server
	address string
	logger  *slog.Logger
	mu      sync.RWMutex
	started bool
}

// NewGRPCProvider creates a new gRPC provider with a gRPC server ready
// for service registration. Call Start() to begin listening.
func NewGRPCProvider(address string, logger *slog.Logger) *GRPCProvider {
	if address == "" {
		address = ":50051"
	}

	server := grpc.NewServer()

	return &GRPCProvider{
		server:  server,
		address: address,
		logger:  logger,
	}
}

// RegisterService registers a gRPC service on the shared server.
// Other extensions (like remora) call this during their Init() to
// register their gRPC services before the server starts.
func (p *GRPCProvider) RegisterService(desc *grpc.ServiceDesc, impl interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		p.logger.Warn("attempted to register service after server started", "service", desc.ServiceName)
		return
	}

	p.server.RegisterService(desc, impl)
	p.logger.Info("gRPC service registered", "service", desc.ServiceName)
}

// Start starts the gRPC server listening on the configured address.
// This blocks until the server stops or an error occurs.
func (p *GRPCProvider) Start() error {
	p.mu.Lock()
	p.started = true
	p.mu.Unlock()

	lis, err := net.Listen("tcp", p.address)
	if err != nil {
		return err
	}

	p.logger.Info("gRPC server listening", "address", p.address)
	return p.server.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (p *GRPCProvider) Stop() {
	p.logger.Info("gRPC server stopping")
	p.server.GracefulStop()
}

// Server returns the underlying gRPC server for direct registration
// by extensions that need the full grpc.ServiceDesc.
func (p *GRPCProvider) Server() *grpc.Server {
	return p.server
}

// Address returns the configured listen address.
func (p *GRPCProvider) Address() string {
	return p.address
}
