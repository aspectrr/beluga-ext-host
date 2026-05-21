# beluga-ext-host

The gRPC host extension for [Beluga](https://github.com/aspectrr/beluga). Provides the shared gRPC server that other extensions (like remora) can register services on, and accepts remote extension connections over a bidirectional gRPC stream.

## What It Does

- Starts a gRPC server at a configurable address (default `:50051`)
- Registers the `ExtensionHostService` — allows remote extension processes to connect, register tools, and execute them over gRPC
- Exposes a `GRPCProvider` that other compiled-in extensions (like remora) can use to register their own gRPC services

## Install

```bash
beluga extend install github.com/aspectrr/beluga-ext-host
```

## Config

```yaml
extensions:
  ext_host:
    enabled: true
    address: ":50051"    # gRPC listen address
    tls_cert: ""         # optional TLS cert path
    tls_key: ""          # optional TLS key path
```

## Remote Extensions

Remote extensions connect to ext_host's gRPC server using the [beluga-ext-sdk](https://github.com/aspectrr/beluga-ext-sdk) client package. See that repo for usage examples.
