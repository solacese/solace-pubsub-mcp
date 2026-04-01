package com.solace.mcp.model;

public record SubscribeRequest(
        String topic,
        String clientSubscriptionId
) {}
