package com.solace.mcp.subscribe;

import com.solace.mcp.config.SolaceMcpConfig;
import com.solace.mcp.connection.BrokerConnectionManager;
import com.solace.mcp.error.ConnectivityException;
import com.solace.mcp.error.SubscriptionException;
import com.solace.mcp.metrics.MetricsService;
import com.solace.mcp.model.*;
import com.solace.mcp.policy.TopicPolicyEngine;
import com.solacesystems.jcsmp.*;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages session-scoped topic subscriptions with bounded per-subscription buffers.
 *
 * Subscription delivery model (dual-path):
 *
 * 1. PUSH (resource notifications):
 *    When a message arrives from the broker, the server fires
 *    notifications/resources/updated for the subscription's resource URI.
 *    The MCP client receives the notification and reads the resource to get messages.
 *    Latency ≈ MCP notification round-trip.
 *
 * 2. POLL (receive_messages tool):
 *    The client can also explicitly poll buffered messages at any time.
 *    Both paths coexist — the push notification triggers the client to read,
 *    while poll works for clients that don't support resource subscriptions.
 *
 * Each subscription has a bounded ArrayBlockingQueue buffer.
 * On buffer overflow, the oldest message is dropped (drop-head policy).
 * Subscriptions are ephemeral and do not survive server restart.
 * On broker reconnect, JCSMP reapplies subscriptions automatically (REAPPLY_SUBSCRIPTIONS=true).
 */
public class SolaceSubscriptionManager implements XMLMessageListener {

    private static final Logger log = LoggerFactory.getLogger(SolaceSubscriptionManager.class);

    public static final String RESOURCE_URI_PREFIX = "solace://subscriptions/";
    public static final String RESOURCE_URI_SUFFIX = "/messages";

    private final BrokerConnectionManager connectionManager;
    private final TopicPolicyEngine policyEngine;
    private final MetricsService metrics;
    private final int maxActiveSubscriptions;
    private final int maxBufferPerSubscription;

    private final ConcurrentHashMap<String, SubscriptionEntry> subscriptions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> topicToSubscriptionIds = new ConcurrentHashMap<>();

    private volatile XMLMessageConsumer consumer;
    private volatile McpSyncServer mcpServer;

    public SolaceSubscriptionManager(
            BrokerConnectionManager connectionManager,
            TopicPolicyEngine policyEngine,
            MetricsService metrics,
            SolaceMcpConfig.LimitsConfig limits) {
        this.connectionManager = connectionManager;
        this.policyEngine = policyEngine;
        this.metrics = metrics;
        this.maxActiveSubscriptions = limits.maxActiveSubscriptions();
        this.maxBufferPerSubscription = limits.maxBufferPerSubscription();
    }

    /**
     * Sets the MCP server reference so we can fire resource-updated notifications.
     * Must be called after the MCP server is built but before messages arrive.
     */
    public void setMcpServer(McpSyncServer mcpServer) {
        this.mcpServer = mcpServer;
    }

    public void init() {
        try {
            this.consumer = connectionManager.getSession().getMessageConsumer(this);
            consumer.start();
            log.info("Solace message consumer started");
        } catch (JCSMPException e) {
            throw new ConnectivityException("Failed to create message consumer", e);
        }
    }

    public SubscribeResult subscribe(SubscribeRequest request) {
        metrics.recordSubscribeRequest();

        policyEngine.validateForSubscribe(request.topic());

        if (subscriptions.size() >= maxActiveSubscriptions) {
            throw new SubscriptionException(
                    "Maximum active subscriptions (%d) reached".formatted(maxActiveSubscriptions));
        }

        // Check for duplicate topic subscription
        for (SubscriptionEntry entry : subscriptions.values()) {
            if (entry.topic().equals(request.topic())) {
                throw new SubscriptionException(
                        "Already subscribed to topic '%s' (subscriptionId=%s)".formatted(
                                request.topic(), entry.subscriptionId()));
            }
        }

        String subscriptionId = request.clientSubscriptionId() != null && !request.clientSubscriptionId().isBlank()
                ? request.clientSubscriptionId()
                : UUID.randomUUID().toString();

        if (subscriptions.containsKey(subscriptionId)) {
            throw new SubscriptionException("Subscription ID '%s' already exists".formatted(subscriptionId));
        }

        try {
            Topic topic = JCSMPFactory.onlyInstance().createTopic(request.topic());
            connectionManager.getSession().addSubscription(topic);

            var entry = new SubscriptionEntry(subscriptionId, request.topic(), maxBufferPerSubscription);
            subscriptions.put(subscriptionId, entry);
            topicToSubscriptionIds.computeIfAbsent(request.topic(), k -> ConcurrentHashMap.newKeySet())
                    .add(subscriptionId);

            metrics.incrementActiveSubscriptions();
            connectionManager.recordSuccessfulInteraction();

            // Register a dynamic MCP resource for this subscription
            registerSubscriptionResource(subscriptionId, request.topic());

            log.info("Subscribed to topic '{}', subscriptionId={}, resourceUri={}",
                    request.topic(), subscriptionId, resourceUri(subscriptionId));

            return new SubscribeResult(
                    true,
                    subscriptionId,
                    request.topic(),
                    entry.createdAt(),
                    "bounded-buffer, capacity=%d, overflow=drop-oldest, push=resource-notification".formatted(maxBufferPerSubscription)
            );
        } catch (JCSMPException e) {
            throw new SubscriptionException(
                    "Failed to subscribe to topic '%s': %s".formatted(request.topic(), e.getMessage()), e);
        }
    }

    public UnsubscribeResult unsubscribe(String subscriptionId) {
        metrics.recordUnsubscribeRequest();

        SubscriptionEntry entry = subscriptions.remove(subscriptionId);
        if (entry == null) {
            throw new SubscriptionException("Subscription '%s' not found".formatted(subscriptionId));
        }

        // Remove from topic mapping
        Set<String> ids = topicToSubscriptionIds.get(entry.topic());
        if (ids != null) {
            ids.remove(subscriptionId);
            if (ids.isEmpty()) {
                topicToSubscriptionIds.remove(entry.topic());
            }
        }

        boolean otherSubscriptionsExist = subscriptions.values().stream()
                .anyMatch(e -> e.topic().equals(entry.topic()));

        if (!otherSubscriptionsExist) {
            try {
                Topic topic = JCSMPFactory.onlyInstance().createTopic(entry.topic());
                connectionManager.getSession().removeSubscription(topic);
            } catch (JCSMPException e) {
                log.warn("Failed to remove broker subscription for topic '{}': {}",
                        entry.topic(), e.getMessage());
            }
        }

        // Remove the dynamic MCP resource
        removeSubscriptionResource(subscriptionId);

        metrics.decrementActiveSubscriptions();
        connectionManager.recordSuccessfulInteraction();

        log.info("Unsubscribed from topic '{}', subscriptionId={}", entry.topic(), subscriptionId);

        return new UnsubscribeResult(true, subscriptionId, Instant.now());
    }

    public ReceiveMessagesResult receiveMessages(ReceiveMessagesRequest request) {
        SubscriptionEntry entry = subscriptions.get(request.subscriptionId());
        if (entry == null) {
            throw new SubscriptionException(
                    "Subscription '%s' not found".formatted(request.subscriptionId()));
        }

        int maxMessages = request.maxMessages() != null ? request.maxMessages() : 10;
        maxMessages = Math.min(Math.max(maxMessages, 1), 100);

        long waitTimeMs = request.waitTimeMs() != null ? request.waitTimeMs() : 0;
        waitTimeMs = Math.min(Math.max(waitTimeMs, 0), 30_000);

        List<InboundMessage> messages;
        if (waitTimeMs > 0) {
            messages = entry.drainWithWait(maxMessages, waitTimeMs);
        } else {
            messages = entry.drain(maxMessages);
        }

        connectionManager.recordSuccessfulInteraction();

        return new ReceiveMessagesResult(
                request.subscriptionId(),
                messages,
                entry.bufferedCount()
        );
    }

    /**
     * Reads buffered messages for a subscription. Used by the MCP resource read handler.
     * This is the same as receiveMessages but called via the resource path.
     */
    public List<InboundMessage> readMessagesForResource(String subscriptionId) {
        SubscriptionEntry entry = subscriptions.get(subscriptionId);
        if (entry == null) return List.of();
        return entry.drain(100);
    }

    public List<SubscriptionInfo> listSubscriptions() {
        return subscriptions.values().stream()
                .map(e -> new SubscriptionInfo(
                        e.subscriptionId(),
                        e.topic(),
                        e.createdAt(),
                        e.messagesReceived(),
                        e.bufferedCount()))
                .toList();
    }

    public int getActiveCount() {
        return subscriptions.size();
    }

    @Override
    public void onReceive(BytesXMLMessage message) {
        metrics.recordMessageReceived();

        String topic = message.getDestination().getName();
        Instant receivedAt = Instant.now();

        // Extract payload
        Object payload;
        if (message instanceof TextMessage textMsg && textMsg.getText() != null) {
            payload = textMsg.getText();
        } else if (message instanceof BytesMessage bytesMsg && bytesMsg.getData() != null) {
            payload = new String(bytesMsg.getData(), java.nio.charset.StandardCharsets.UTF_8);
        } else {
            payload = "";
        }

        // Extract properties
        Map<String, String> properties = new LinkedHashMap<>();
        if (message.getProperties() != null) {
            try {
                SDTMap props = message.getProperties();
                for (String key : props.keySet()) {
                    properties.put(key, String.valueOf(props.get(key)));
                }
            } catch (SDTException e) {
                log.debug("Error reading message properties: {}", e.getMessage());
            }
        }

        routeMessage(topic, payload, message.getHTTPContentType(), message.getCorrelationId(),
                properties, receivedAt);
    }

    @Override
    public void onException(JCSMPException e) {
        log.error("Message consumer exception: {}", e.getMessage());
    }

    private void routeMessage(String topic, Object payload, String contentType,
                              String correlationId, Map<String, String> properties, Instant receivedAt) {
        boolean routed = false;

        for (SubscriptionEntry entry : subscriptions.values()) {
            if (topicMatchesSubscription(topic, entry.topic())) {
                InboundMessage inbound = new InboundMessage(
                        entry.subscriptionId(), topic, payload, contentType,
                        correlationId, properties, receivedAt);

                boolean accepted = entry.offer(inbound);
                if (!accepted) {
                    log.debug("Buffer overflow for subscription '{}' on topic '{}'",
                            entry.subscriptionId(), topic);
                }
                routed = true;

                // Fire resource-updated notification to MCP client
                notifyResourceUpdated(entry.subscriptionId());
            }
        }

        if (!routed) {
            log.trace("No subscription matched for topic '{}'", topic);
        }
    }

    /**
     * Sends a notifications/resources/updated to the MCP client so it knows
     * new messages are available on this subscription's resource.
     */
    private void notifyResourceUpdated(String subscriptionId) {
        McpSyncServer server = this.mcpServer;
        if (server == null) return;

        try {
            server.notifyResourcesUpdated(
                    new McpSchema.ResourcesUpdatedNotification(resourceUri(subscriptionId), null));
            log.trace("Sent resource-updated notification for subscription '{}'", subscriptionId);
        } catch (Exception e) {
            // Non-fatal: client may not support resource subscriptions
            log.debug("Failed to send resource-updated notification for '{}': {}",
                    subscriptionId, e.getMessage());
        }
    }

    /**
     * Registers a dynamic MCP resource for a subscription so clients can
     * subscribe to resource updates and read messages via resources/read.
     */
    private void registerSubscriptionResource(String subscriptionId, String topicPattern) {
        McpSyncServer server = this.mcpServer;
        if (server == null) return;

        try {
            var resource = McpSchema.Resource.builder()
                    .uri(resourceUri(subscriptionId))
                    .name("Subscription: " + topicPattern)
                    .description("Buffered messages for subscription " + subscriptionId + " on topic " + topicPattern)
                    .mimeType("application/json")
                    .build();

            var spec = new McpServerFeatures.SyncResourceSpecification(
                    resource,
                    (exchange, request) -> {
                        List<InboundMessage> messages = readMessagesForResource(subscriptionId);
                        String json = serializeMessages(messages);
                        return new McpSchema.ReadResourceResult(
                                List.of(new McpSchema.TextResourceContents(
                                        request.uri(), "application/json", json, null)),
                                null);
                    });

            server.addResource(spec);
            server.notifyResourcesListChanged();
            log.debug("Registered MCP resource for subscription '{}'", subscriptionId);
        } catch (Exception e) {
            log.warn("Failed to register MCP resource for subscription '{}': {}",
                    subscriptionId, e.getMessage());
        }
    }

    /**
     * Removes the dynamic MCP resource when a subscription is cancelled.
     */
    private void removeSubscriptionResource(String subscriptionId) {
        McpSyncServer server = this.mcpServer;
        if (server == null) return;

        try {
            server.removeResource(resourceUri(subscriptionId));
            server.notifyResourcesListChanged();
            log.debug("Removed MCP resource for subscription '{}'", subscriptionId);
        } catch (Exception e) {
            log.debug("Failed to remove MCP resource for subscription '{}': {}",
                    subscriptionId, e.getMessage());
        }
    }

    private String serializeMessages(List<InboundMessage> messages) {
        // Simple JSON array serialization without pulling in ObjectMapper dependency
        if (messages.isEmpty()) return "[]";
        var sb = new StringBuilder("[");
        for (int i = 0; i < messages.size(); i++) {
            if (i > 0) sb.append(",");
            var m = messages.get(i);
            sb.append("{\"subscriptionId\":\"").append(escape(m.subscriptionId())).append("\"");
            sb.append(",\"topic\":\"").append(escape(m.topic())).append("\"");
            sb.append(",\"payload\":").append(payloadToJson(m.payload()));
            if (m.contentType() != null) {
                sb.append(",\"contentType\":\"").append(escape(m.contentType())).append("\"");
            }
            if (m.correlationId() != null) {
                sb.append(",\"correlationId\":\"").append(escape(m.correlationId())).append("\"");
            }
            if (m.properties() != null && !m.properties().isEmpty()) {
                sb.append(",\"properties\":{");
                int j = 0;
                for (var entry : m.properties().entrySet()) {
                    if (j++ > 0) sb.append(",");
                    sb.append("\"").append(escape(entry.getKey())).append("\":\"").append(escape(entry.getValue())).append("\"");
                }
                sb.append("}");
            }
            sb.append(",\"receivedAt\":\"").append(m.receivedAt()).append("\"");
            sb.append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    private static String payloadToJson(Object payload) {
        if (payload == null) return "null";
        String s = payload.toString();
        // If it looks like JSON object/array, pass through directly
        if ((s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"))) {
            return s;
        }
        return "\"" + escape(s) + "\"";
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    static String resourceUri(String subscriptionId) {
        return RESOURCE_URI_PREFIX + subscriptionId + RESOURCE_URI_SUFFIX;
    }

    static boolean topicMatchesSubscription(String topic, String subscriptionPattern) {
        if (topic.equals(subscriptionPattern)) return true;

        String[] topicParts = topic.split("/", -1);
        String[] patternParts = subscriptionPattern.split("/", -1);

        for (int i = 0; i < patternParts.length; i++) {
            if (patternParts[i].equals(">")) {
                return i < topicParts.length;
            }
            if (i >= topicParts.length) return false;
            if (patternParts[i].equals("*")) continue;
            if (!patternParts[i].equals(topicParts[i])) return false;
        }
        return topicParts.length == patternParts.length;
    }

    public void close() {
        if (consumer != null) {
            consumer.stop();
            consumer.close();
        }
        subscriptions.clear();
        topicToSubscriptionIds.clear();
    }
}
