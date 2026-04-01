package com.solace.mcp.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.solace.mcp.model.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the model serialization and tool response structures.
 * Service-level mocking is omitted because Mockito does not support
 * the inline mock-maker on Java 25 without full agent setup.
 * Integration with the actual tool handlers is tested via the full
 * service graph in integration tests.
 */
class McpToolHandlersTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Test
    void publishResultSerializesToJson() throws Exception {
        var result = new PublishResult(true, "test/topic", "msg-123", "corr-1", Instant.now());
        String json = objectMapper.writeValueAsString(result);
        assertThat(json).contains("\"success\":true");
        assertThat(json).contains("\"topic\":\"test/topic\"");
        assertThat(json).contains("\"messageId\":\"msg-123\"");
        assertThat(json).contains("\"correlationId\":\"corr-1\"");
    }

    @Test
    void subscribeResultSerializesToJson() throws Exception {
        var result = new SubscribeResult(true, "sub-abc", "orders/*", Instant.now(), "bounded-buffer");
        String json = objectMapper.writeValueAsString(result);
        assertThat(json).contains("\"success\":true");
        assertThat(json).contains("\"subscriptionId\":\"sub-abc\"");
        assertThat(json).contains("\"topic\":\"orders/*\"");
    }

    @Test
    void connectionStatusSerializesToJson() throws Exception {
        var status = new ConnectionStatus(true, "tcp://broker:55555", "default", false, "CONNECTED", 3600, 0, 3);
        String json = objectMapper.writeValueAsString(status);
        assertThat(json).contains("\"connected\":true");
        assertThat(json).contains("\"state\":\"CONNECTED\"");
        assertThat(json).contains("\"activeSubscriptionCount\":3");
    }

    @Test
    void healthStatusSerializesToJson() throws Exception {
        var health = new HealthStatus("UP", true, Instant.now(), "CONNECTED", Map.of("publishSuccess", 42L));
        String json = objectMapper.writeValueAsString(health);
        assertThat(json).contains("\"status\":\"UP\"");
        assertThat(json).contains("\"publishSuccess\":42");
    }

    @Test
    void requestReplyResultSerializesToJson() throws Exception {
        var result = new RequestReplyResult(true, "svc/topic", "corr-1", "response-data", "text/plain", Map.of(), 150);
        String json = objectMapper.writeValueAsString(result);
        assertThat(json).contains("\"success\":true");
        assertThat(json).contains("\"requestTopic\":\"svc/topic\"");
        assertThat(json).contains("\"durationMs\":150");
    }

    @Test
    void inboundMessageSerializesToJson() throws Exception {
        var msg = new InboundMessage("sub-1", "orders/new", "{\"id\":1}", "application/json", null, Map.of("key", "val"), Instant.now());
        String json = objectMapper.writeValueAsString(msg);
        assertThat(json).contains("\"subscriptionId\":\"sub-1\"");
        assertThat(json).contains("\"topic\":\"orders/new\"");
        assertThat(json).contains("\"key\":\"val\"");
    }

    @Test
    void receiveMessagesResultSerializesToJson() throws Exception {
        var msg = new InboundMessage("sub-1", "t", "data", null, null, Map.of(), Instant.now());
        var result = new ReceiveMessagesResult("sub-1", java.util.List.of(msg), 5);
        String json = objectMapper.writeValueAsString(result);
        assertThat(json).contains("\"remainingBuffered\":5");
        assertThat(json).contains("\"messages\":[");
    }

    @Test
    void unsubscribeResultSerializesToJson() throws Exception {
        var result = new UnsubscribeResult(true, "sub-1", Instant.now());
        String json = objectMapper.writeValueAsString(result);
        assertThat(json).contains("\"success\":true");
        assertThat(json).contains("\"subscriptionId\":\"sub-1\"");
    }
}
