package com.solace.mcp.model;

import java.time.Instant;
import java.util.Map;

public record HealthStatus(
        String status,
        boolean connected,
        Instant lastSuccessfulInteraction,
        String reconnectState,
        Map<String, Long> metrics
) {}
