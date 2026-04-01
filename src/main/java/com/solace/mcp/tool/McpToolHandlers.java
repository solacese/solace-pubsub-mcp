package com.solace.mcp.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.mcp.error.SolaceMcpException;
import com.solace.mcp.health.HealthService;
import com.solace.mcp.model.*;
import com.solace.mcp.publish.SolacePublisher;
import com.solace.mcp.requestreply.RequestReplyService;
import com.solace.mcp.subscribe.SolaceSubscriptionManager;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Registers all MCP tools and maps tool calls to service layer operations.
 * Each handler validates input, delegates to a service, serializes the result,
 * and converts exceptions to safe MCP error responses.
 */
public final class McpToolHandlers {

    private static final Logger log = LoggerFactory.getLogger(McpToolHandlers.class);

    private final SolacePublisher publisher;
    private final SolaceSubscriptionManager subscriptionManager;
    private final RequestReplyService requestReplyService;
    private final HealthService healthService;
    private final ObjectMapper objectMapper;

    public McpToolHandlers(
            SolacePublisher publisher,
            SolaceSubscriptionManager subscriptionManager,
            RequestReplyService requestReplyService,
            HealthService healthService,
            ObjectMapper objectMapper) {
        this.publisher = publisher;
        this.subscriptionManager = subscriptionManager;
        this.requestReplyService = requestReplyService;
        this.healthService = healthService;
        this.objectMapper = objectMapper;
    }

    public void registerAll(McpSyncServer server) {
        server.addTool(publishEventTool());
        server.addTool(subscribeTopicTool());
        server.addTool(unsubscribeTopicTool());
        server.addTool(receiveMessagesTool());
        server.addTool(requestReplyTool());
        server.addTool(connectionStatusTool());
        server.addTool(healthTool());
        server.addTool(listSubscriptionsTool());
        log.info("Registered {} MCP tools", 8);
    }

    private McpServerFeatures.SyncToolSpecification publishEventTool() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "topic": { "type": "string", "description": "Solace topic to publish to" },
                    "payload": { "description": "Message payload (JSON object or string)" },
                    "contentType": { "type": "string", "description": "Content type (e.g. application/json)" },
                    "correlationId": { "type": "string", "description": "Correlation ID for message tracking" },
                    "properties": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Custom message properties" }
                  },
                  "required": ["topic", "payload"]
                }
                """;

        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(Tool.builder()
                        .name("publish_event")
                        .description("Publish a message to a Solace PubSub+ topic")
                        .inputSchema(parseSchema(schema))
                        .build())
                .callHandler((exchange, request) -> {
                    try {
                        var args = request.arguments();
                        var req = new PublishRequest(
                                getString(args, "topic"),
                                args.get("payload"),
                                getString(args, "contentType"),
                                getString(args, "correlationId"),
                                getStringMap(args, "properties")
                        );
                        PublishResult result = publisher.publish(req);
                        return successResult(result);
                    } catch (SolaceMcpException e) {
                        return errorResult(e);
                    } catch (Exception e) {
                        return internalError(e);
                    }
                })
                .build();
    }

    private McpServerFeatures.SyncToolSpecification subscribeTopicTool() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "topic": { "type": "string", "description": "Solace topic pattern to subscribe to" },
                    "clientSubscriptionId": { "type": "string", "description": "Optional client-chosen subscription ID" }
                  },
                  "required": ["topic"]
                }
                """;

        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(Tool.builder()
                        .name("subscribe_topic")
                        .description("Subscribe to a Solace PubSub+ topic. Messages are buffered and retrieved via receive_messages.")
                        .inputSchema(parseSchema(schema))
                        .build())
                .callHandler((exchange, request) -> {
                    try {
                        var args = request.arguments();
                        var req = new SubscribeRequest(
                                getString(args, "topic"),
                                getString(args, "clientSubscriptionId")
                        );
                        SubscribeResult result = subscriptionManager.subscribe(req);
                        return successResult(result);
                    } catch (SolaceMcpException e) {
                        return errorResult(e);
                    } catch (Exception e) {
                        return internalError(e);
                    }
                })
                .build();
    }

    private McpServerFeatures.SyncToolSpecification unsubscribeTopicTool() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "subscriptionId": { "type": "string", "description": "Subscription ID to unsubscribe" }
                  },
                  "required": ["subscriptionId"]
                }
                """;

        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(Tool.builder()
                        .name("unsubscribe_topic")
                        .description("Unsubscribe from a previously subscribed Solace PubSub+ topic")
                        .inputSchema(parseSchema(schema))
                        .build())
                .callHandler((exchange, request) -> {
                    try {
                        String subId = getString(request.arguments(), "subscriptionId");
                        if (subId == null || subId.isBlank()) {
                            return errorResult("VALIDATION_ERROR", "subscriptionId is required");
                        }
                        UnsubscribeResult result = subscriptionManager.unsubscribe(subId);
                        return successResult(result);
                    } catch (SolaceMcpException e) {
                        return errorResult(e);
                    } catch (Exception e) {
                        return internalError(e);
                    }
                })
                .build();
    }

    private McpServerFeatures.SyncToolSpecification receiveMessagesTool() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "subscriptionId": { "type": "string", "description": "Subscription ID to receive messages from" },
                    "maxMessages": { "type": "integer", "description": "Maximum messages to return (1-100, default 10)" },
                    "waitTimeMs": { "type": "integer", "description": "Time to wait for messages if buffer is empty (0-30000ms, default 0)" }
                  },
                  "required": ["subscriptionId"]
                }
                """;

        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(Tool.builder()
                        .name("receive_messages")
                        .description("Receive buffered messages from a subscription. Returns immediately if messages are available, or waits up to waitTimeMs.")
                        .inputSchema(parseSchema(schema))
                        .build())
                .callHandler((exchange, request) -> {
                    try {
                        var args = request.arguments();
                        var req = new ReceiveMessagesRequest(
                                getString(args, "subscriptionId"),
                                getInteger(args, "maxMessages"),
                                getLong(args, "waitTimeMs")
                        );
                        ReceiveMessagesResult result = subscriptionManager.receiveMessages(req);
                        return successResult(result);
                    } catch (SolaceMcpException e) {
                        return errorResult(e);
                    } catch (Exception e) {
                        return internalError(e);
                    }
                })
                .build();
    }

    private McpServerFeatures.SyncToolSpecification requestReplyTool() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "topic": { "type": "string", "description": "Solace topic to send the request to" },
                    "payload": { "description": "Request payload (JSON object or string)" },
                    "contentType": { "type": "string", "description": "Content type" },
                    "correlationId": { "type": "string", "description": "Correlation ID" },
                    "properties": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Custom properties" },
                    "timeoutMs": { "type": "integer", "description": "Timeout in milliseconds" }
                  },
                  "required": ["topic", "payload"]
                }
                """;

        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(Tool.builder()
                        .name("request_reply")
                        .description("Send a request to a Solace PubSub+ topic and wait for a reply")
                        .inputSchema(parseSchema(schema))
                        .build())
                .callHandler((exchange, request) -> {
                    try {
                        var args = request.arguments();
                        var req = new RequestReplyRequest(
                                getString(args, "topic"),
                                args.get("payload"),
                                getString(args, "contentType"),
                                getString(args, "correlationId"),
                                getStringMap(args, "properties"),
                                getLong(args, "timeoutMs")
                        );
                        RequestReplyResult result = requestReplyService.sendRequest(req);
                        return successResult(result);
                    } catch (SolaceMcpException e) {
                        return errorResult(e);
                    } catch (Exception e) {
                        return internalError(e);
                    }
                })
                .build();
    }

    private McpServerFeatures.SyncToolSpecification connectionStatusTool() {
        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(Tool.builder()
                        .name("connection_status")
                        .description("Get current Solace PubSub+ broker connection status")
                        .inputSchema(parseSchema("""
                                { "type": "object", "properties": {} }
                                """))
                        .build())
                .callHandler((exchange, request) -> {
                    try {
                        ConnectionStatus status = healthService.getConnectionStatus();
                        return successResult(status);
                    } catch (Exception e) {
                        return internalError(e);
                    }
                })
                .build();
    }

    private McpServerFeatures.SyncToolSpecification healthTool() {
        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(Tool.builder()
                        .name("health")
                        .description("Get health status and metrics for the Solace PubSub MCP server")
                        .inputSchema(parseSchema("""
                                { "type": "object", "properties": {} }
                                """))
                        .build())
                .callHandler((exchange, request) -> {
                    try {
                        HealthStatus health = healthService.getHealth();
                        return successResult(health);
                    } catch (Exception e) {
                        return internalError(e);
                    }
                })
                .build();
    }

    private McpServerFeatures.SyncToolSpecification listSubscriptionsTool() {
        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(Tool.builder()
                        .name("list_subscriptions")
                        .description("List all active topic subscriptions")
                        .inputSchema(parseSchema("""
                                { "type": "object", "properties": {} }
                                """))
                        .build())
                .callHandler((exchange, request) -> {
                    try {
                        var subs = subscriptionManager.listSubscriptions();
                        return successResult(Map.of("subscriptions", subs));
                    } catch (Exception e) {
                        return internalError(e);
                    }
                })
                .build();
    }

    // --- Helpers ---

    private McpSchema.JsonSchema parseSchema(String json) {
        try {
            return objectMapper.readValue(json, McpSchema.JsonSchema.class);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid tool schema: " + e.getMessage(), e);
        }
    }

    private CallToolResult successResult(Object result) {
        try {
            String json = objectMapper.writeValueAsString(result);
            return CallToolResult.builder()
                    .content(List.of(new McpSchema.TextContent(json)))
                    .build();
        } catch (Exception e) {
            return internalError(e);
        }
    }

    private CallToolResult errorResult(SolaceMcpException e) {
        log.warn("Tool error [{}]: {}", e.errorCode().code(), e.getMessage());
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("error", true);
        error.put("code", e.errorCode().code());
        error.put("message", e.getMessage());
        try {
            return CallToolResult.builder()
                    .content(List.of(new McpSchema.TextContent(objectMapper.writeValueAsString(error))))
                    .isError(true)
                    .build();
        } catch (Exception ex) {
            return CallToolResult.builder()
                    .content(List.of(new McpSchema.TextContent("{\"error\":true,\"code\":\"INTERNAL_ERROR\"}")))
                    .isError(true)
                    .build();
        }
    }

    private CallToolResult errorResult(String code, String message) {
        Map<String, Object> error = Map.of("error", true, "code", code, "message", message);
        try {
            return CallToolResult.builder()
                    .content(List.of(new McpSchema.TextContent(objectMapper.writeValueAsString(error))))
                    .isError(true)
                    .build();
        } catch (Exception ex) {
            return CallToolResult.builder()
                    .content(List.of(new McpSchema.TextContent("{\"error\":true}")))
                    .isError(true)
                    .build();
        }
    }

    private CallToolResult internalError(Exception e) {
        log.error("Internal error in tool handler: {}", e.getMessage(), e);
        return CallToolResult.builder()
                .content(List.of(new McpSchema.TextContent(
                        "{\"error\":true,\"code\":\"INTERNAL_ERROR\",\"message\":\"An internal error occurred\"}")))
                .isError(true)
                .build();
    }

    private static String getString(Map<String, Object> args, String key) {
        Object value = args.get(key);
        return value != null ? value.toString() : null;
    }

    private static Integer getInteger(Map<String, Object> args, String key) {
        Object value = args.get(key);
        if (value == null) return null;
        if (value instanceof Number num) return num.intValue();
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long getLong(Map<String, Object> args, String key) {
        Object value = args.get(key);
        if (value == null) return null;
        if (value instanceof Number num) return num.longValue();
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getStringMap(Map<String, Object> args, String key) {
        Object value = args.get(key);
        if (value instanceof Map<?, ?> map) {
            var result = new LinkedHashMap<String, String>();
            map.forEach((k, v) -> result.put(k.toString(), v != null ? v.toString() : ""));
            return result;
        }
        return null;
    }
}
