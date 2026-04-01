package com.solace.mcp.model;

import java.util.Map;

public record RequestReplyResult(
        boolean success,
        String requestTopic,
        String correlationId,
        Object responsePayload,
        String responseContentType,
        Map<String, String> responseProperties,
        long durationMs
) {}
