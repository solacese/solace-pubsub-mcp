package com.solace.mcp.subscribe;

import com.solace.mcp.config.SolaceMcpConfig;
import com.solace.mcp.connection.BrokerConnectionManager;
import com.solace.mcp.error.ConnectivityException;
import com.solace.mcp.error.SubscriptionException;
import com.solace.mcp.metrics.MetricsService;
import com.solace.mcp.model.*;
import com.solace.mcp.policy.TopicPolicyEngine;
import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages session-scoped topic subscriptions with bounded per-subscription buffers.
 *
 * Subscription delivery model:
 * - Each subscription has a bounded ArrayBlockingQueue buffer.
 * - Inbound messages are dispatched to matching subscriptions by topic.
 * - MCP clients poll for messages via the receive_messages tool.
 * - On buffer overflow, the oldest message is dropped (drop-head policy).
 * - Subscriptions are ephemeral and do not survive server restart.
 * - On broker reconnect, JCSMP reapplies subscriptions automatically (REAPPLY_SUBSCRIPTIONS=true).
 */
public class SolaceSubscriptionManager implements XMLMessageListener {

    private static final Logger log = LoggerFactory.getLogger(SolaceSubscriptionManager.class);

    private final BrokerConnectionManager connectionManager;
    private final TopicPolicyEngine policyEngine;
    private final MetricsService metrics;
    private final int maxActiveSubscriptions;
    private final int maxBufferPerSubscription;

    private final ConcurrentHashMap<String, SubscriptionEntry> subscriptions = new ConcurrentHashMap<>();
    // Maps Solace topic string to subscription IDs for message routing
    private final ConcurrentHashMap<String, Set<String>> topicToSubscriptionIds = new ConcurrentHashMap<>();

    private volatile XMLMessageConsumer consumer;

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

            log.info("Subscribed to topic '{}', subscriptionId={}", request.topic(), subscriptionId);

            return new SubscribeResult(
                    true,
                    subscriptionId,
                    request.topic(),
                    entry.createdAt(),
                    "bounded-buffer, capacity=%d, overflow=drop-oldest".formatted(maxBufferPerSubscription)
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

        // Only remove the broker subscription if no other subscription entries use the same topic
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

        // Route to all matching subscriptions
        // Use direct topic match first, then check wildcard subscriptions
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
            }
        }

        if (!routed) {
            log.trace("No subscription matched for topic '{}'", topic);
        }
    }

    /**
     * Checks if a concrete topic matches a subscription pattern.
     * Handles Solace wildcards: * = single level, > = one or more trailing levels.
     */
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
