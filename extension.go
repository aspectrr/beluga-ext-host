package ext_host

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	sdk "github.com/aspectrr/beluga-ext-sdk/belugav1"
	"github.com/collinpfeifer/beluga/pkg/extension"
)

// Extension provides the shared gRPC server for other extensions (remora)
// and accepts remote extension connections. Core Beluga has no gRPC code —
// this extension adds it.
type Extension struct {
	provider *GRPCProvider
	logger   *slog.Logger
}

// Config holds the ext_host configuration from beluga.yaml.
type Config struct {
	Address string `json:"address"`
	TLSCert string `json:"tls_cert"`
	TLSKey  string `json:"tls_key"`
}

func (e *Extension) Name() string { return "ext_host" }

func (e *Extension) Init(ctx extension.ExtensionContext) error {
	// Parse config.
	var cfg Config
	if ctx.Config != nil {
		if err := json.Unmarshal(ctx.Config, &cfg); err != nil {
			return fmt.Errorf("parsing ext_host config: %w", err)
		}
	}

	e.logger = ctx.Logger

	// Create the gRPC provider.
	e.provider = NewGRPCProvider(cfg.Address, ctx.Logger)

	// Register the remote extension host service on our server.
	remoteExtServer := NewRemoteExtServer(e.provider, ctx.Registry, ctx.Logger)
	sdk.RegisterExtensionHostServiceServer(e.provider.Server(), remoteExtServer)

	// Set the GRPC field on the shared ExtensionContext so later
	// extensions (like remora) can discover it. Since extensions
	// get copies of the context, we use the pointer approach:
	// if ctx.GRPC is a **GRPCProvider, we set the inner pointer.
	if ctx.GRPC != nil {
		if ptr, ok := ctx.GRPC.(**GRPCProvider); ok {
			*ptr = e.provider
		}
	}

	ctx.Logger.Info("ext_host initialized", "address", cfg.Address)
	return nil
}

func (e *Extension) Start(ctx context.Context) error {
	// Start the gRPC server. This blocks until the server stops.
	if err := e.provider.Start(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("gRPC server error: %w", err)
	}
	return nil
}

func (e *Extension) Stop(ctx context.Context) error {
	if e.provider != nil {
		e.provider.Stop()
	}
	return nil
}
