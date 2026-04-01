package com.solace.mcp.model;

import java.time.Instant;

public record SubscribeResult(
        boolean success,
        String subscriptionId,
        String topic,
        Instant createdAt,
        String bufferingPolicy
) {}
