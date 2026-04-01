package com.solace.mcp.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralized metrics for Solace PubSub MCP.
 * Uses Micrometer for structured counters and timers.
 */
public final class MetricsService {

    private final MeterRegistry registry;

    private final Counter publishAttempts;
    private final Counter publishSuccess;
    private final Counter publishFailure;
    private final Counter subscribeRequests;
    private final Counter unsubscribeRequests;
    private final Counter messagesReceived;
    private final Counter reconnectAttempts;
    private final Counter requestReplyAttempts;
    private final Counter requestReplySuccess;
    private final Counter requestReplyTimeout;
    private final Timer requestReplyLatency;
    private final AtomicLong activeSubscriptions = new AtomicLong(0);

    public MetricsService(boolean enabled) {
        this.registry = enabled ? new SimpleMeterRegistry() : new SimpleMeterRegistry();

        this.publishAttempts = Counter.builder("solace.mcp.publish.attempts").register(registry);
        this.publishSuccess = Counter.builder("solace.mcp.publish.success").register(registry);
        this.publishFailure = Counter.builder("solace.mcp.publish.failure").register(registry);
        this.subscribeRequests = Counter.builder("solace.mcp.subscribe.requests").register(registry);
        this.unsubscribeRequests = Counter.builder("solace.mcp.unsubscribe.requests").register(registry);
        this.messagesReceived = Counter.builder("solace.mcp.messages.received").register(registry);
        this.reconnectAttempts = Counter.builder("solace.mcp.reconnect.attempts").register(registry);
        this.requestReplyAttempts = Counter.builder("solace.mcp.requestreply.attempts").register(registry);
        this.requestReplySuccess = Counter.builder("solace.mcp.requestreply.success").register(registry);
        this.requestReplyTimeout = Counter.builder("solace.mcp.requestreply.timeout").register(registry);
        this.requestReplyLatency = Timer.builder("solace.mcp.requestreply.latency").register(registry);
    }

    public void recordPublishAttempt() { publishAttempts.increment(); }
    public void recordPublishSuccess() { publishSuccess.increment(); }
    public void recordPublishFailure() { publishFailure.increment(); }
    public void recordSubscribeRequest() { subscribeRequests.increment(); }
    public void recordUnsubscribeRequest() { unsubscribeRequests.increment(); }
    public void recordMessageReceived() { messagesReceived.increment(); }
    public void recordReconnectAttempt() { reconnectAttempts.increment(); }
    public void recordRequestReplyAttempt() { requestReplyAttempts.increment(); }
    public void recordRequestReplySuccess() { requestReplySuccess.increment(); }
    public void recordRequestReplyTimeout() { requestReplyTimeout.increment(); }
    public Timer getRequestReplyLatency() { return requestReplyLatency; }

    public void incrementActiveSubscriptions() { activeSubscriptions.incrementAndGet(); }
    public void decrementActiveSubscriptions() { activeSubscriptions.decrementAndGet(); }
    public long getActiveSubscriptionCount() { return activeSubscriptions.get(); }

    public Map<String, Long> snapshot() {
        var map = new LinkedHashMap<String, Long>();
        map.put("publishAttempts", (long) publishAttempts.count());
        map.put("publishSuccess", (long) publishSuccess.count());
        map.put("publishFailure", (long) publishFailure.count());
        map.put("subscribeRequests", (long) subscribeRequests.count());
        map.put("unsubscribeRequests", (long) unsubscribeRequests.count());
        map.put("messagesReceived", (long) messagesReceived.count());
        map.put("reconnectAttempts", (long) reconnectAttempts.count());
        map.put("requestReplyAttempts", (long) requestReplyAttempts.count());
        map.put("requestReplySuccess", (long) requestReplySuccess.count());
        map.put("requestReplyTimeout", (long) requestReplyTimeout.count());
        map.put("activeSubscriptions", activeSubscriptions.get());
        return map;
    }
}
