package com.solace.mcp.model;

import java.time.Instant;

public record SubscriptionInfo(
        String subscriptionId,
        String topic,
        Instant createdAt,
        long messagesReceived,
        int bufferedMessages
) {}
