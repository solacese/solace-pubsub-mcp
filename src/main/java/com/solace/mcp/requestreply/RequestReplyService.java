package com.solace.mcp.requestreply;

import com.solace.mcp.connection.BrokerConnectionManager;
import com.solace.mcp.error.ConnectivityException;
import com.solace.mcp.error.RequestTimeoutException;
import com.solace.mcp.metrics.MetricsService;
import com.solace.mcp.model.RequestReplyRequest;
import com.solace.mcp.model.RequestReplyResult;
import com.solace.mcp.policy.TopicPolicyEngine;
import com.solace.mcp.publish.PayloadSerializer;
import com.solacesystems.jcsmp.*;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Implements Solace request/reply pattern using JCSMP Requestor.
 * Thread-safe. Each request creates a correlation and waits for a reply with timeout.
 */
public class RequestReplyService {

    private static final Logger log = LoggerFactory.getLogger(RequestReplyService.class);

    private final BrokerConnectionManager connectionManager;
    private final TopicPolicyEngine policyEngine;
    private final PayloadSerializer serializer;
    private final MetricsService metrics;
    private final long defaultTimeoutMs;
    private final long maxTimeoutMs;

    public RequestReplyService(
            BrokerConnectionManager connectionManager,
            TopicPolicyEngine policyEngine,
            PayloadSerializer serializer,
            MetricsService metrics,
            long defaultTimeoutMs) {
        this.connectionManager = connectionManager;
        this.policyEngine = policyEngine;
        this.serializer = serializer;
        this.metrics = metrics;
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.maxTimeoutMs = Math.max(defaultTimeoutMs, 60_000);
    }

    public RequestReplyResult sendRequest(RequestReplyRequest request) {
        metrics.recordRequestReplyAttempt();

        policyEngine.validateForPublish(request.topic());

        byte[] payload = serializer.serialize(request.payload(), request.contentType());

        long timeoutMs = request.timeoutMs() != null ? request.timeoutMs() : defaultTimeoutMs;
        timeoutMs = Math.min(Math.max(timeoutMs, 100), maxTimeoutMs);

        String correlationId = request.correlationId() != null && !request.correlationId().isBlank()
                ? request.correlationId()
                : UUID.randomUUID().toString();

        Timer.Sample sample = Timer.start();

        try {
            Requestor requestor = connectionManager.getSession().createRequestor();

            BytesMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            requestMsg.setData(payload);
            requestMsg.setCorrelationId(correlationId);

            if (request.contentType() != null && !request.contentType().isBlank()) {
                requestMsg.setHTTPContentType(request.contentType());
            }
            if (request.properties() != null && !request.properties().isEmpty()) {
                SDTMap userProps = JCSMPFactory.onlyInstance().createMap();
                for (Map.Entry<String, String> entry : request.properties().entrySet()) {
                    userProps.putString(entry.getKey(), entry.getValue());
                }
                requestMsg.setProperties(userProps);
            }

            Topic topic = JCSMPFactory.onlyInstance().createTopic(request.topic());
            BytesXMLMessage replyMsg = requestor.request(requestMsg, (int) timeoutMs, topic);

            long durationMs = sample.stop(metrics.getRequestReplyLatency()) / 1_000_000;
            metrics.recordRequestReplySuccess();
            connectionManager.recordSuccessfulInteraction();

            // Extract reply payload
            Object responsePayload;
            if (replyMsg instanceof TextMessage textReply && textReply.getText() != null) {
                responsePayload = textReply.getText();
            } else if (replyMsg instanceof BytesMessage bytesReply && bytesReply.getData() != null) {
                responsePayload = new String(bytesReply.getData(), StandardCharsets.UTF_8);
            } else {
                responsePayload = "";
            }

            // Extract reply properties
            Map<String, String> responseProperties = new LinkedHashMap<>();
            if (replyMsg.getProperties() != null) {
                try {
                    SDTMap props = replyMsg.getProperties();
                    for (String key : props.keySet()) {
                        responseProperties.put(key, String.valueOf(props.get(key)));
                    }
                } catch (SDTException e) {
                    log.debug("Error reading reply properties: {}", e.getMessage());
                }
            }

            log.debug("Request/reply completed for topic '{}' in {}ms", request.topic(), durationMs);

            return new RequestReplyResult(
                    true,
                    request.topic(),
                    correlationId,
                    responsePayload,
                    replyMsg.getHTTPContentType(),
                    responseProperties,
                    durationMs
            );
        } catch (JCSMPRequestTimeoutException e) {
            sample.stop(metrics.getRequestReplyLatency());
            metrics.recordRequestReplyTimeout();
            throw new RequestTimeoutException(
                    "Request to topic '%s' timed out after %dms".formatted(request.topic(), timeoutMs));
        } catch (JCSMPException e) {
            sample.stop(metrics.getRequestReplyLatency());
            throw new ConnectivityException(
                    "Request/reply failed for topic '%s': %s".formatted(request.topic(), e.getMessage()), e);
        }
    }
}
