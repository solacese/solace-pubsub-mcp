# Solace PubSub MCP

A production-grade [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server that bridges MCP clients to [Solace PubSub+](https://solace.com/products/platform/) brokers.

## Why the MCP surface is intentionally minimal

This server exposes exactly **8 tools** — publish, subscribe, unsubscribe, receive messages, request/reply, connection status, health, and list subscriptions. That's it.

It is not a broker admin platform. It does not expose queue provisioning, ACL management, user management, or any dangerous introspection features. The goal is a narrow, robust, enterprise-ready bridge between MCP clients and Solace brokers with strong topic policy enforcement and secure defaults.

## Features

- **Publish** messages to Solace topics with JSON or text payloads, custom headers, and correlation IDs
- **Subscribe** to topics with wildcard support, bounded per-subscription message buffering, and overflow protection
- **Request/Reply** with configurable timeouts and structured response envelopes
- **Topic policy enforcement** — allowlists, denylists, wildcard controls, full-tree wildcard protection
- **Secure by default** — secrets via environment variables, no credential leakage in logs or errors, payload size limits, subscription count limits
- **Observable** — structured logging (SLF4J + Logback), Micrometer metrics for publish/subscribe/reconnect/latency
- **Reliable** — automatic reconnect with exponential backoff, subscription reapplication on reconnect, graceful shutdown
- **Portable** — runs as a JAR, in Docker, on Kubernetes, or embedded in any MCP-compatible client

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  MCP Client (Claude Desktop, IDE, etc.)                 │
│                          │ STDIO (JSON-RPC)             │
├──────────────────────────┼──────────────────────────────┤
│  SolacePubSubMcpServer   │                              │
│  ┌───────────────────────┴────────────────────────────┐ │
│  │ McpToolHandlers                                    │ │
│  │  publish_event │ subscribe_topic │ request_reply   │ │
│  │  unsubscribe   │ receive_messages│ health/status   │ │
│  └────────┬───────┴────────┬────────┴────────┬────────┘ │
│           │                │                 │          │
│  ┌────────▼──┐  ┌──────────▼──────┐  ┌──────▼───────┐  │
│  │ Publisher  │  │ SubscriptionMgr │  │ ReqReply Svc │  │
│  └────────┬──┘  └──────────┬──────┘  └──────┬───────┘  │
│           │                │                 │          │
│  ┌────────▼────────────────▼─────────────────▼───────┐  │
│  │ TopicPolicyEngine  │  PayloadSerializer           │  │
│  └────────┬────────────────┬─────────────────────────┘  │
│           │                │                            │
│  ┌────────▼────────────────▼─────────────────────────┐  │
│  │ BrokerConnectionManager (JCSMP Session)           │  │
│  └───────────────────────┬───────────────────────────┘  │
│                          │                              │
├──────────────────────────┼──────────────────────────────┤
│  Solace PubSub+ Broker   │ SMF / SMF-TLS               │
└──────────────────────────┴──────────────────────────────┘
```

### Subscription delivery model

Subscriptions are **session-scoped and ephemeral**. Each subscription gets a bounded `ArrayBlockingQueue` buffer. Inbound messages are routed by topic matching (including Solace wildcards `*` and `>`). MCP clients retrieve messages by calling the `receive_messages` tool with the subscription ID.

- **Overflow policy**: drop-oldest (the oldest buffered message is discarded to make room)
- **Reconnect**: JCSMP reapplies subscriptions automatically (`REAPPLY_SUBSCRIPTIONS=true`)
- **Cleanup**: subscriptions are removed on unsubscribe or server shutdown

## Tools

| Tool | Description |
|------|-------------|
| `publish_event` | Publish a message to a topic |
| `subscribe_topic` | Subscribe to a topic pattern |
| `unsubscribe_topic` | Remove a subscription |
| `receive_messages` | Poll for buffered messages from a subscription |
| `request_reply` | Send a request and wait for a reply |
| `connection_status` | Get broker connection status |
| `health` | Get server health and metrics |
| `list_subscriptions` | List active subscriptions |

## Configuration

All configuration is via environment variables. See [`.env.example`](.env.example) for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `SOLACE_HOST` | `tcp://localhost:55555` | Broker host URL |
| `SOLACE_VPN` | `default` | Message VPN |
| `SOLACE_USERNAME` | `default` | Username |
| `SOLACE_PASSWORD` | `default` | Password |
| `SOLACE_TLS_ENABLED` | `false` | Enable TLS |
| `SOLACE_RECONNECT_ENABLED` | `true` | Auto-reconnect |
| `SOLACE_MAX_PAYLOAD_BYTES` | `1048576` | Max payload size (1MB) |
| `SOLACE_MAX_ACTIVE_SUBSCRIPTIONS` | `50` | Max concurrent subscriptions |
| `SOLACE_MAX_BUFFER_PER_SUBSCRIPTION` | `1000` | Max buffered messages per subscription |
| `SOLACE_REQUEST_TIMEOUT_MS` | `30000` | Request/reply timeout |
| `SOLACE_TOPIC_ALLOW_LIST` | *(empty)* | Comma-separated topic patterns to allow |
| `SOLACE_TOPIC_DENY_LIST` | *(empty)* | Comma-separated topic patterns to deny |
| `SOLACE_ALLOW_WILDCARD_SUBSCRIPTIONS` | `true` | Allow `*` wildcards |
| `SOLACE_ALLOW_FULL_WILDCARD` | `false` | Allow `>` (full tree) wildcard |
| `SOLACE_METRICS_ENABLED` | `true` | Enable metrics collection |

### Topic policy

- **Deny list overrides allow list** — if a topic matches a deny pattern, it is always rejected
- **Empty allow list = allow all** (that aren't denied)
- Full-tree wildcard (`>`) is **blocked by default** and must be explicitly enabled

## Security notes

- Passwords are read from environment variables only, never from command-line arguments
- Credentials are never logged — all error messages and log output redact secrets
- Stack traces are never returned to MCP clients — errors are mapped to structured codes
- Topic policy is enforced before every publish and subscribe
- Payload size limits prevent abuse
- Subscription count limits prevent resource exhaustion
- Default settings are conservative

## Running locally

### Prerequisites

- Java 21+
- Maven 3.9+
- A Solace PubSub+ broker (use Docker Compose below for a local one)

### Start a local broker

```bash
docker compose up -d solace
```

### Build and run

```bash
mvn package -DskipTests
java -jar target/solace-pubsub-mcp-0.1.0.jar
```

### Run tests

```bash
mvn test
```

## Docker usage

```bash
docker build -t solace-pubsub-mcp .
docker run -i --rm \
  -e SOLACE_HOST=tcp://host.docker.internal:55555 \
  -e SOLACE_VPN=default \
  -e SOLACE_USERNAME=default \
  -e SOLACE_PASSWORD=default \
  solace-pubsub-mcp
```

## MCP client configuration

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "solace": {
      "command": "java",
      "args": ["-jar", "/path/to/solace-pubsub-mcp-0.1.0.jar"],
      "env": {
        "SOLACE_HOST": "tcp://localhost:55555",
        "SOLACE_VPN": "default",
        "SOLACE_USERNAME": "default",
        "SOLACE_PASSWORD": "default"
      }
    }
  }
}
```

### Claude Code

Add to `.claude/settings.json`:

```json
{
  "mcpServers": {
    "solace": {
      "command": "java",
      "args": ["-jar", "/path/to/solace-pubsub-mcp-0.1.0.jar"],
      "env": {
        "SOLACE_HOST": "tcp://localhost:55555",
        "SOLACE_VPN": "default",
        "SOLACE_USERNAME": "default",
        "SOLACE_PASSWORD": "default"
      }
    }
  }
}
```

## Example tool calls

### Publish an event

```json
{
  "tool": "publish_event",
  "arguments": {
    "topic": "orders/new",
    "payload": {"orderId": "12345", "amount": 99.99},
    "contentType": "application/json",
    "correlationId": "req-001"
  }
}
```

### Subscribe to a topic

```json
{
  "tool": "subscribe_topic",
  "arguments": {
    "topic": "orders/*"
  }
}
```

### Receive messages

```json
{
  "tool": "receive_messages",
  "arguments": {
    "subscriptionId": "sub-abc-123",
    "maxMessages": 5,
    "waitTimeMs": 5000
  }
}
```

### Request/Reply

```json
{
  "tool": "request_reply",
  "arguments": {
    "topic": "services/inventory/check",
    "payload": {"sku": "WIDGET-42"},
    "timeoutMs": 10000
  }
}
```

## Technology choices

| Component | Choice | Reason |
|-----------|--------|--------|
| Build | Maven | Standard enterprise build tool, straightforward dependency management |
| Java | 21 | Virtual threads if needed later, modern records/patterns |
| MCP SDK | `io.modelcontextprotocol.sdk:mcp:1.1.1` | Official SDK, standalone STDIO transport, no Spring required |
| Solace client | `com.solacesystems:sol-jcsmp:10.29.1` | Battle-tested, full request/reply via `Requestor`, fine-grained subscription control, automatic reapply |
| JSON | Jackson | MCP SDK already depends on it, JSR310 module for Instant serialization |
| Logging | SLF4J + Logback | Standard, stderr output keeps stdout clean for MCP STDIO |
| Metrics | Micrometer | Lightweight counters/timers, no server needed for SimpleMeterRegistry |
| Tests | JUnit 5 + Mockito + AssertJ | Modern, expressive |

## Limitations

- **STDIO transport only** — SSE/HTTP transport can be added later
- **No durable subscriptions** — session-scoped only (by design for safety)
- **No queue consumption** — direct topic subscriptions only
- **Poll-based message delivery** — MCP's sync request/response model doesn't support server-push; clients call `receive_messages` to retrieve buffered messages
- **Single broker connection** — no multi-broker fan-out

## Future extensions

These can be added without expanding the default tool surface:

- SSE or Streamable HTTP transport for web-based MCP clients
- Queue-backed durable subscriptions (opt-in, behind config flag)
- Schema validation for published payloads
- Rate limiting with token bucket
- mTLS support
- Multi-VPN connections
- Prometheus metrics exporter endpoint
- OpenTelemetry tracing
