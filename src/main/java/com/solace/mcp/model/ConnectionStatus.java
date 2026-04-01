package com.solace.mcp.model;

public record ConnectionStatus(
        boolean connected,
        String brokerHost,
        String vpn,
        boolean tlsEnabled,
        String state,
        long uptimeSeconds,
        long reconnectCount,
        int activeSubscriptionCount
) {}
