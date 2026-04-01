package com.solace.mcp.model;

import java.util.Map;

public record RequestReplyRequest(
        String topic,
        Object payload,
        String contentType,
        String correlationId,
        Map<String, String> properties,
        Long timeoutMs
) {}
