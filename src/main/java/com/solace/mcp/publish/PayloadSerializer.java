package com.solace.mcp.publish;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.mcp.error.PayloadValidationException;

import java.nio.charset.StandardCharsets;

/**
 * Serializes MCP tool payloads into byte arrays for Solace messages.
 */
public final class PayloadSerializer {

    private final ObjectMapper objectMapper;
    private final int maxPayloadBytes;

    public PayloadSerializer(ObjectMapper objectMapper, int maxPayloadBytes) {
        this.objectMapper = objectMapper;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public byte[] serialize(Object payload, String contentType) {
        if (payload == null) {
            throw new PayloadValidationException("Payload must not be null");
        }

        byte[] bytes;
        if (payload instanceof String text) {
            bytes = text.getBytes(StandardCharsets.UTF_8);
        } else {
            try {
                bytes = objectMapper.writeValueAsBytes(payload);
            } catch (JsonProcessingException e) {
                throw new PayloadValidationException("Failed to serialize payload to JSON: " + e.getMessage());
            }
        }

        if (bytes.length == 0) {
            throw new PayloadValidationException("Payload must not be empty");
        }
        if (bytes.length > maxPayloadBytes) {
            throw new PayloadValidationException(
                    "Payload size %d bytes exceeds maximum %d bytes".formatted(bytes.length, maxPayloadBytes));
        }

        return bytes;
    }

    public String deserialize(byte[] data) {
        if (data == null || data.length == 0) return "";
        return new String(data, StandardCharsets.UTF_8);
    }
}
