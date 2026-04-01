package com.solace.mcp.publish;

import com.solace.mcp.connection.BrokerConnectionManager;
import com.solace.mcp.error.ConnectivityException;
import com.solace.mcp.metrics.MetricsService;
import com.solace.mcp.model.PublishRequest;
import com.solace.mcp.model.PublishResult;
import com.solace.mcp.policy.TopicPolicyEngine;
import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Publishes messages to Solace topics via JCSMP.
 * Thread-safe. Reuses the shared session producer.
 */
public class SolacePublisher {

    private static final Logger log = LoggerFactory.getLogger(SolacePublisher.class);

    private final BrokerConnectionManager connectionManager;
    private final TopicPolicyEngine policyEngine;
    private final PayloadSerializer serializer;
    private final MetricsService metrics;

    private volatile XMLMessageProducer producer;

    public SolacePublisher(
            BrokerConnectionManager connectionManager,
            TopicPolicyEngine policyEngine,
            PayloadSerializer serializer,
            MetricsService metrics) {
        this.connectionManager = connectionManager;
        this.policyEngine = policyEngine;
        this.serializer = serializer;
        this.metrics = metrics;
    }

    public void init() {
        try {
            this.producer = connectionManager.getSession().getMessageProducer(
                    new JCSMPStreamingPublishCorrelatingEventHandler() {
                        @Override
                        public void responseReceivedEx(Object correlationKey) {
                            log.trace("Publish ACK received for: {}", correlationKey);
                        }

                        @Override
                        public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
                            log.error("Publish NACK for {}: {}", correlationKey, cause.getMessage());
                        }
                    });
            log.info("Solace publisher initialized");
        } catch (JCSMPException e) {
            throw new ConnectivityException("Failed to create message producer", e);
        }
    }

    public PublishResult publish(PublishRequest request) {
        metrics.recordPublishAttempt();

        policyEngine.validateForPublish(request.topic());

        byte[] payload = serializer.serialize(request.payload(), request.contentType());
        String messageId = UUID.randomUUID().toString();

        try {
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            message.setData(payload);
            message.setApplicationMessageId(messageId);

            if (request.contentType() != null && !request.contentType().isBlank()) {
                message.setHTTPContentType(request.contentType());
            }
            if (request.correlationId() != null && !request.correlationId().isBlank()) {
                message.setCorrelationId(request.correlationId());
            }
            if (request.properties() != null && !request.properties().isEmpty()) {
                SDTMap userProps = JCSMPFactory.onlyInstance().createMap();
                for (Map.Entry<String, String> entry : request.properties().entrySet()) {
                    userProps.putString(entry.getKey(), entry.getValue());
                }
                message.setProperties(userProps);
            }

            Topic topic = JCSMPFactory.onlyInstance().createTopic(request.topic());
            producer.send(message, topic);

            connectionManager.recordSuccessfulInteraction();
            metrics.recordPublishSuccess();

            log.debug("Published to topic '{}', messageId={}", request.topic(), messageId);

            return new PublishResult(
                    true,
                    request.topic(),
                    messageId,
                    request.correlationId(),
                    Instant.now()
            );
        } catch (JCSMPException e) {
            metrics.recordPublishFailure();
            throw new ConnectivityException("Failed to publish to topic '%s': %s".formatted(
                    request.topic(), e.getMessage()), e);
        }
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
