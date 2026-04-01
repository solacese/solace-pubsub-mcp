package com.solace.mcp.model;

public record ReceiveMessagesRequest(
        String subscriptionId,
        Integer maxMessages,
        Long waitTimeMs
) {}
