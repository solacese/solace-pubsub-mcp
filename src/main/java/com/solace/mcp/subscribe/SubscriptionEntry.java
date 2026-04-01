package com.solace.mcp.subscribe;

import com.solace.mcp.model.InboundMessage;

import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks state for a single active subscription including its bounded message buffer.
 */
public final class SubscriptionEntry {

    private final String subscriptionId;
    private final String topic;
    private final Instant createdAt;
    private final BlockingQueue<InboundMessage> buffer;
    private final AtomicLong messagesReceived = new AtomicLong(0);

    public SubscriptionEntry(String subscriptionId, String topic, int bufferCapacity) {
        this.subscriptionId = subscriptionId;
        this.topic = topic;
        this.createdAt = Instant.now();
        this.buffer = new ArrayBlockingQueue<>(bufferCapacity);
    }

    public String subscriptionId() { return subscriptionId; }
    public String topic() { return topic; }
    public Instant createdAt() { return createdAt; }
    public long messagesReceived() { return messagesReceived.get(); }
    public int bufferedCount() { return buffer.size(); }

    /**
     * Offers a message to the buffer. If full, drops the oldest message to make room.
     * Returns true if accepted, false if the message was dropped due to overflow.
     */
    public boolean offer(InboundMessage message) {
        messagesReceived.incrementAndGet();
        if (!buffer.offer(message)) {
            // Drop oldest to make room (bounded buffer overflow policy)
            buffer.poll();
            buffer.offer(message);
            return false; // indicates overflow occurred
        }
        return true;
    }

    /**
     * Drains up to maxMessages from the buffer.
     */
    public java.util.List<InboundMessage> drain(int maxMessages) {
        var messages = new java.util.ArrayList<InboundMessage>(Math.min(maxMessages, buffer.size()));
        buffer.drainTo(messages, maxMessages);
        return messages;
    }

    /**
     * Drains up to maxMessages, waiting up to waitTimeMs for at least one message.
     */
    public java.util.List<InboundMessage> drainWithWait(int maxMessages, long waitTimeMs) {
        var messages = new java.util.ArrayList<InboundMessage>(maxMessages);

        // Wait for at least one message if buffer is empty
        if (buffer.isEmpty() && waitTimeMs > 0) {
            try {
                InboundMessage first = buffer.poll(waitTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (first != null) {
                    messages.add(first);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return messages;
            }
        }

        // Drain any additional available messages
        if (messages.size() < maxMessages) {
            buffer.drainTo(messages, maxMessages - messages.size());
        }
        return messages;
    }
}
