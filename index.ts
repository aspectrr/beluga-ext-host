// ── Extension Host (gRPC) ─────────────────────────────────────
// Ported from github.com/aspectrr/beluga-ext-host (Go → TypeScript)
//
// Starts a gRPC server for remote extension processes and remora daemons.
// Uses @grpc/proto-loader to load the proto at runtime — no protoc needed.
// Exposes the gRPC provider via ctx.shared so remora can register services.

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import type {
	Extension,
	ExtensionContext,
	Tool,
	ToolDef,
	ToolContext,
	Registry,
} from "@aspectrr/beluga-sdk";

// ── Proto loading ──────────────────────────────────────────────

const PROTO_PATH = resolve(
	dirname(fileURLToPath(import.meta.url)),
	"proto",
	"remora.proto",
);

const packageDef = protoLoader.loadSync(PROTO_PATH, {
	keepCase: true,
	longs: String,
	enums: String,
	defaults: true,
	oneofs: true,
});

const belugaProto = grpc.loadPackageDefinition(packageDef)
	.beluga as unknown as {
	v1: {
		ExtensionHostService: grpc.ServiceClientConstructor;
		RemoraService: grpc.ServiceClientConstructor;
	};
};

// ── GRPC Provider Interface ──────────────────────────────────
// Defined here — not in @beluga/sdk. Only extensions that need gRPC
// (remora) import from this extension.

export interface GRPCProvider {
	registerService(descriptor: unknown, implementation: unknown): void;
	start(): Promise<void>;
	stop(): void;
}

// ── GRPC Provider Implementation ──────────────────────────────

export class GRPCProviderImpl implements GRPCProvider {
	server: grpc.Server;
	address: string;
	private logger: import("pino").Logger;
	private started = false;

	constructor(address: string, logger: import("pino").Logger) {
		this.address = address || ":50051";
		this.logger = logger;
		this.server = new grpc.Server();
	}

	registerService(descriptor: unknown, implementation: unknown): void {
		const desc = descriptor as grpc.ServiceClientConstructor;
		this.server.addService(
			(desc as unknown as { service: grpc.ServiceDefinition }).service ?? desc,
			implementation as grpc.UntypedServiceImplementation,
		);
		this.logger.info(
			{ service: (desc as unknown as { serviceName?: string }).serviceName },
			"gRPC service registered",
		);
	}

	async start(): Promise<void> {
		return new Promise((resolve, reject) => {
			this.server.bindAsync(
				this.address,
				grpc.ServerCredentials.createInsecure(),
				(err, port) => {
					if (err) {
						reject(err);
						return;
					}
					this.started = true;
					this.server.start();
					this.logger.info(
						{ address: this.address, port },
						"gRPC server listening",
					);
					resolve();
				},
			);
		});
	}

	stop(): void {
		this.logger.info("gRPC server stopping");
		if (this.started) {
			this.server.forceShutdown();
		}
	}
}

// ── Remote Extension Host Service ──────────────────────────────

interface RemoteConnection {
	name: string;
	stream: grpc.ServerDuplexStream<unknown, unknown>;
	tools: string[];
	pending: Map<
		string,
		{
			resolve: (result: Record<string, unknown>) => void;
			reject: (err: Error) => void;
		}
	>;
}

class RemoteExtServer {
	private registry: Registry;
	private logger: import("pino").Logger;
	private connections = new Map<string, RemoteConnection>();

	constructor(
		registry: Registry,
		logger: import("pino").Logger,
	) {
		this.registry = registry;
		this.logger = logger;
	}

	/** ExtensionHostService.Connect — bidirectional stream */
	connect(call: grpc.ServerDuplexStream<unknown, unknown>): void {
		let conn: RemoteConnection | null = null;

		const cleanup = () => {
			if (conn) this.unregisterConnection(conn);
		};

		call.on("data", (msg: unknown) => {
			const m = msg as Record<string, unknown>;
			const payload = (m.payload ?? m) as Record<string, unknown>;

			// Registration message
			if (payload.registration) {
				const reg = payload.registration as Record<string, unknown>;
				const extName = reg.extension_name as string;
				const tools = (reg.tools as Array<Record<string, unknown>>) ?? [];

				conn = {
					name: extName,
					stream: call,
					tools: [],
					pending: new Map(),
				};

				for (const td of tools) {
					const toolName = td.name as string;
					const remoteTool = new RemoteTool(
						toolName,
						td.description as string,
						typeof td.parameters === "string"
							? JSON.parse(td.parameters)
							: (td.parameters as Record<string, unknown>),
						conn,
					);
					this.registry.register(remoteTool);
					conn.tools.push(toolName);
				}

				this.registerConnection(conn);
				this.logger.info(
					{ extension: extName, tools: conn.tools.length },
					"remote extension connected",
				);
			}

			// Tool result message
			if (payload.tool_result) {
				const result = payload.tool_result as Record<string, unknown>;
				const callId = result.call_id as string;
				const pending = conn?.pending.get(callId);
				if (pending) {
					conn!.pending.delete(callId);
					if (result.is_error) {
						pending.reject(
							new Error(`remote tool error: ${result.output}`),
						);
					} else {
						try {
							const parsed =
								typeof result.output === "string"
									? JSON.parse(result.output)
									: result.output;
							pending.resolve(parsed as Record<string, unknown>);
						} catch {
							pending.resolve({ output: result.output });
						}
					}
				}
			}
		});

		call.on("end", cleanup);
		call.on("error", cleanup);
	}

	private registerConnection(conn: RemoteConnection): void {
		const old = this.connections.get(conn.name);
		if (old) {
			this.logger.warn(
				{ extension: conn.name },
				"replacing existing remote extension connection",
			);
			for (const toolName of old.tools) {
				this.registry.unregister(toolName);
			}
		}
		this.connections.set(conn.name, conn);
	}

	private unregisterConnection(conn: RemoteConnection): void {
		this.connections.delete(conn.name);
		for (const toolName of conn.tools) {
			this.registry.unregister(toolName);
		}
		this.logger.info(
			{ extension: conn.name, tools: conn.tools.length },
			"remote extension disconnected",
		);
	}
}

// ── Remote Tool (proxy) ────────────────────────────────────────

class RemoteTool implements Tool {
	private toolName: string;
	private desc: string;
	private params: Record<string, unknown>;
	private conn: RemoteConnection;

	constructor(
		name: string,
		description: string,
		parameters: Record<string, unknown>,
		conn: RemoteConnection,
	) {
		this.toolName = name;
		this.desc = description;
		this.params = parameters;
		this.conn = conn;
	}

	definition(): ToolDef {
		return {
			name: this.toolName,
			description: this.desc,
			parameters: this.params,
		};
	}

	async execute(
		args: Record<string, unknown>,
		ctx: ToolContext,
	): Promise<Record<string, unknown>> {
		const callId = `${ctx.sessionId}-${this.toolName}`;

		return new Promise((resolve, reject) => {
			this.conn.pending.set(callId, { resolve, reject });

			this.conn.stream.write({
				payload: {
					execute_tool: {
						call_id: callId,
						tool_name: this.toolName,
						arguments: JSON.stringify(args),
					},
				},
			});

			// Timeout after 60s
			setTimeout(() => {
				if (this.conn.pending.has(callId)) {
					this.conn.pending.delete(callId);
					reject(new Error(`remote tool ${this.toolName} timed out`));
				}
			}, 60_000);
		});
	}
}

// ── Host Extension ─────────────────────────────────────────────

class HostExtension implements Extension {
	name = "ext_host";
	private provider?: GRPCProviderImpl;

	async init(ctx: ExtensionContext): Promise<void> {
		const cfg = ctx.config as { address?: string };
		const address = cfg.address || ":50051";

		this.provider = new GRPCProviderImpl(address, ctx.logger);

		// Register ExtensionHostService on the gRPC server
		const remoteExtServer = new RemoteExtServer(ctx.registry, ctx.logger);
		this.provider.registerService(belugaProto.v1.ExtensionHostService, {
			connect: remoteExtServer.connect.bind(remoteExtServer),
		});

		// Expose provider in shared context so remora can register its service
		ctx.shared.grpcProvider = this.provider;

		ctx.logger.info({ address }, "ext_host initialized");
	}

	async start(_signal: AbortSignal): Promise<void> {
		await this.provider?.start();
	}

	async stop(): Promise<void> {
		this.provider?.stop();
	}
}

export default new HostExtension();
