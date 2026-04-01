package com.solace.mcp.model;

import java.time.Instant;

public record UnsubscribeResult(
        boolean success,
        String subscriptionId,
        Instant unsubscribedAt
) {}
