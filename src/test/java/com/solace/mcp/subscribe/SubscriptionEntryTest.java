package com.solace.mcp.subscribe;

import com.solace.mcp.model.InboundMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class SubscriptionEntryTest {

    @Test
    void bufferAcceptsMessages() {
        var entry = new SubscriptionEntry("sub-1", "test/topic", 10);

        boolean accepted = entry.offer(makeMessage("sub-1", "test/topic"));
        assertThat(accepted).isTrue();
        assertThat(entry.bufferedCount()).isEqualTo(1);
        assertThat(entry.messagesReceived()).isEqualTo(1);
    }

    @Test
    void bufferOverflowDropsOldest() {
        var entry = new SubscriptionEntry("sub-1", "test/topic", 2);

        entry.offer(makeMessage("sub-1", "msg-1"));
        entry.offer(makeMessage("sub-1", "msg-2"));
        boolean thirdAccepted = entry.offer(makeMessage("sub-1", "msg-3"));

        assertThat(thirdAccepted).isFalse(); // overflow occurred
        assertThat(entry.bufferedCount()).isEqualTo(2);
        assertThat(entry.messagesReceived()).isEqualTo(3);

        var drained = entry.drain(10);
        assertThat(drained).hasSize(2);
        // Oldest was dropped, so we get msg-2 and msg-3
        assertThat(drained.get(0).payload()).isEqualTo("msg-2");
        assertThat(drained.get(1).payload()).isEqualTo("msg-3");
    }

    @Test
    void drainReturnsUpToMaxMessages() {
        var entry = new SubscriptionEntry("sub-1", "test/topic", 100);
        for (int i = 0; i < 20; i++) {
            entry.offer(makeMessage("sub-1", "msg-" + i));
        }

        var drained = entry.drain(5);
        assertThat(drained).hasSize(5);
        assertThat(entry.bufferedCount()).isEqualTo(15);
    }

    @Test
    void drainEmptyBufferReturnsEmpty() {
        var entry = new SubscriptionEntry("sub-1", "test/topic", 10);
        var drained = entry.drain(10);
        assertThat(drained).isEmpty();
    }

    private static InboundMessage makeMessage(String subId, String payload) {
        return new InboundMessage(subId, "test/topic", payload, "text/plain", null, Map.of(), Instant.now());
    }
}
