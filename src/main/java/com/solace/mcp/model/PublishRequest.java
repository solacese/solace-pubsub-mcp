package com.solace.mcp.model;

import java.util.Map;

public record PublishRequest(
        String topic,
        Object payload,
        String contentType,
        String correlationId,
        Map<String, String> properties
) {}
