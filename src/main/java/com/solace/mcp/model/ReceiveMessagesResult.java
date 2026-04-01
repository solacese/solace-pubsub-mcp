package com.solace.mcp.model;

import java.util.List;

public record ReceiveMessagesResult(
        String subscriptionId,
        List<InboundMessage> messages,
        int remainingBuffered
) {}
