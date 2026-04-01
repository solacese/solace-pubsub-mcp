package com.solace.mcp.publish;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.mcp.error.PayloadValidationException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class PayloadSerializerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final PayloadSerializer serializer = new PayloadSerializer(mapper, 1024);

    @Test
    void serializesStringPayload() {
        byte[] result = serializer.serialize("hello world", "text/plain");
        assertThat(new String(result)).isEqualTo("hello world");
    }

    @Test
    void serializesJsonObjectPayload() {
        byte[] result = serializer.serialize(Map.of("key", "value"), "application/json");
        assertThat(new String(result)).contains("\"key\"").contains("\"value\"");
    }

    @Test
    void rejectsNullPayload() {
        assertThatThrownBy(() -> serializer.serialize(null, null))
                .isInstanceOf(PayloadValidationException.class)
                .hasMessageContaining("null");
    }

    @Test
    void rejectsEmptyStringPayload() {
        assertThatThrownBy(() -> serializer.serialize("", null))
                .isInstanceOf(PayloadValidationException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void rejectsOversizedPayload() {
        var smallSerializer = new PayloadSerializer(mapper, 10);
        assertThatThrownBy(() -> smallSerializer.serialize("this is too large", null))
                .isInstanceOf(PayloadValidationException.class)
                .hasMessageContaining("exceeds maximum");
    }

    @Test
    void deserializesBytes() {
        String result = serializer.deserialize("test".getBytes());
        assertThat(result).isEqualTo("test");
    }

    @Test
    void deserializesNullToEmpty() {
        assertThat(serializer.deserialize(null)).isEmpty();
    }
}
