package com.solace.mcp.model;

import java.time.Instant;

public record PublishResult(
        boolean success,
        String topic,
        String messageId,
        String correlationId,
        Instant timestamp
) {}
