package com.solace.mcp.model;

import java.time.Instant;
import java.util.Map;

/**
 * Normalized envelope for messages received via subscriptions.
 */
public record InboundMessage(
        String subscriptionId,
        String topic,
        Object payload,
        String contentType,
        String correlationId,
        Map<String, String> properties,
        Instant receivedAt
) {}
