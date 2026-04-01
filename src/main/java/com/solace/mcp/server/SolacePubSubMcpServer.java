package com.solace.mcp.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.solace.mcp.config.ConfigurationLoader;
import com.solace.mcp.config.SolaceMcpConfig;
import com.solace.mcp.connection.BrokerConnectionManager;
import com.solace.mcp.health.HealthService;
import com.solace.mcp.metrics.MetricsService;
import com.solace.mcp.policy.TopicPolicyEngine;
import com.solace.mcp.publish.PayloadSerializer;
import com.solace.mcp.publish.SolacePublisher;
import com.solace.mcp.requestreply.RequestReplyService;
import com.solace.mcp.subscribe.SolaceSubscriptionManager;
import com.solace.mcp.tool.McpToolHandlers;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for Solace PubSub MCP server.
 *
 * Lifecycle:
 * 1. Load configuration from environment
 * 2. Build service graph (connection, publisher, subscriptions, request-reply, health)
 * 3. Connect to Solace broker
 * 4. Start MCP server on STDIO transport
 * 5. Block until shutdown signal
 * 6. Cleanup in reverse order
 */
public final class SolacePubSubMcpServer {

    private static final Logger log = LoggerFactory.getLogger(SolacePubSubMcpServer.class);
    private static final String SERVER_NAME = "solace-pubsub-mcp";
    private static final String SERVER_VERSION = "0.1.0";

    public static void main(String[] args) {
        log.info("Starting {} v{}", SERVER_NAME, SERVER_VERSION);

        SolaceMcpConfig config;
        try {
            config = ConfigurationLoader.load();
        } catch (Exception e) {
            log.error("Configuration error: {}", e.getMessage());
            System.exit(1);
            return;
        }

        // Jackson 2 ObjectMapper for our own serialization (tool responses, payloads)
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Build service graph
        MetricsService metrics = new MetricsService(config.observability().metricsEnabled());
        TopicPolicyEngine policyEngine = new TopicPolicyEngine(config.topicPolicy());
        PayloadSerializer serializer = new PayloadSerializer(objectMapper, config.limits().maxPayloadBytes());
        BrokerConnectionManager connectionManager = new BrokerConnectionManager(config.broker(), metrics);

        SolacePublisher publisher = new SolacePublisher(connectionManager, policyEngine, serializer, metrics);
        SolaceSubscriptionManager subscriptionManager = new SolaceSubscriptionManager(
                connectionManager, policyEngine, metrics, config.limits());
        RequestReplyService requestReplyService = new RequestReplyService(
                connectionManager, policyEngine, serializer, metrics, config.limits().requestTimeoutMs());
        HealthService healthService = new HealthService(connectionManager, subscriptionManager, metrics);

        // Connect to broker
        try {
            connectionManager.connect();
            publisher.init();
            subscriptionManager.init();
        } catch (Exception e) {
            log.error("Failed to connect to Solace broker: {}", e.getMessage());
            System.exit(1);
            return;
        }

        // Set up reconnect callback
        connectionManager.setOnReconnectCallback(() ->
                log.info("Broker reconnected. Subscriptions reapplied by JCSMP (REAPPLY_SUBSCRIPTIONS=true)."));

        // Build MCP server using STDIO transport
        // MCP SDK uses Jackson 3 internally via SPI (McpJsonDefaults.getMapper())
        StdioServerTransportProvider transport = new StdioServerTransportProvider(
                McpJsonDefaults.getMapper(), System.in, System.out);

        McpSyncServer mcpServer = McpServer.sync(transport)
                .serverInfo(SERVER_NAME, SERVER_VERSION)
                .capabilities(McpSchema.ServerCapabilities.builder()
                        .tools(true)
                        .resources(true, true)   // enable resources with subscribe + listChanged
                        .logging()
                        .build())
                .build();

        // Wire the MCP server into the subscription manager for resource notifications
        subscriptionManager.setMcpServer(mcpServer);

        // Register tools
        McpToolHandlers toolHandlers = new McpToolHandlers(
                publisher, subscriptionManager, requestReplyService, healthService, objectMapper);
        toolHandlers.registerAll(mcpServer);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down {} ...", SERVER_NAME);
            try {
                mcpServer.close();
            } catch (Exception e) {
                log.warn("Error closing MCP server: {}", e.getMessage());
            }
            subscriptionManager.close();
            publisher.close();
            connectionManager.disconnect();
            log.info("{} shutdown complete", SERVER_NAME);
        }, "shutdown-hook"));

        log.info("{} v{} ready on STDIO transport", SERVER_NAME, SERVER_VERSION);

        // Block main thread — STDIO transport runs on its own threads
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
