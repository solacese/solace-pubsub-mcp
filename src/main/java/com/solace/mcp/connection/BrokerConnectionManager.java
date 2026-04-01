package com.solace.mcp.connection;

import com.solace.mcp.config.SolaceMcpConfig;
import com.solace.mcp.error.ConnectivityException;
import com.solace.mcp.metrics.MetricsService;
import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages a single JCSMP session to the Solace broker.
 * Handles connection lifecycle, reconnection, and session events.
 * Thread-safe. The session is shared across all tool invocations.
 */
public final class BrokerConnectionManager implements SessionEventHandler {

    private static final Logger log = LoggerFactory.getLogger(BrokerConnectionManager.class);

    private final SolaceMcpConfig.BrokerConfig brokerConfig;
    private final MetricsService metrics;
    private final AtomicReference<JCSMPSession> sessionRef = new AtomicReference<>();
    private final AtomicReference<String> connectionState = new AtomicReference<>("DISCONNECTED");
    private final AtomicLong reconnectCount = new AtomicLong(0);
    private final AtomicReference<Instant> connectedSince = new AtomicReference<>();
    private final AtomicReference<Instant> lastSuccessfulInteraction = new AtomicReference<>();

    private volatile Runnable onReconnectCallback;

    public BrokerConnectionManager(SolaceMcpConfig.BrokerConfig brokerConfig, MetricsService metrics) {
        this.brokerConfig = brokerConfig;
        this.metrics = metrics;
    }

    public void setOnReconnectCallback(Runnable callback) {
        this.onReconnectCallback = callback;
    }

    public void connect() {
        log.info("Connecting to Solace broker: {}", brokerConfig.safeDescription());

        try {
            JCSMPProperties props = new JCSMPProperties();
            props.setProperty(JCSMPProperties.HOST, brokerConfig.host());
            props.setProperty(JCSMPProperties.VPN_NAME, brokerConfig.vpn());
            props.setProperty(JCSMPProperties.USERNAME, brokerConfig.username());
            props.setProperty(JCSMPProperties.PASSWORD, brokerConfig.password());
            props.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);

            if (brokerConfig.tlsEnabled()) {
                props.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, true);
                if (brokerConfig.tlsTrustStorePath() != null) {
                    props.setProperty(JCSMPProperties.SSL_TRUST_STORE, brokerConfig.tlsTrustStorePath());
                }
                if (brokerConfig.tlsTrustStorePassword() != null) {
                    props.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD, brokerConfig.tlsTrustStorePassword());
                }
            }

            var channelProps = new JCSMPChannelProperties();
            if (brokerConfig.reconnect().enabled()) {
                channelProps.setReconnectRetries(brokerConfig.reconnect().retries());
                channelProps.setReconnectRetryWaitInMillis(
                        (int) brokerConfig.reconnect().initialDelayMs());
                channelProps.setConnectRetriesPerHost(5);
            } else {
                channelProps.setReconnectRetries(0);
            }
            props.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);

            JCSMPSession session = JCSMPFactory.onlyInstance().createSession(props, null, this);
            session.connect();

            sessionRef.set(session);
            connectionState.set("CONNECTED");
            connectedSince.set(Instant.now());
            lastSuccessfulInteraction.set(Instant.now());

            log.info("Connected to Solace broker: {}", brokerConfig.safeDescription());
        } catch (JCSMPException e) {
            connectionState.set("FAILED");
            throw new ConnectivityException(
                    "Failed to connect to Solace broker: " + sanitize(e.getMessage()), e);
        }
    }

    public void disconnect() {
        JCSMPSession session = sessionRef.getAndSet(null);
        if (session != null && !session.isClosed()) {
            log.info("Disconnecting from Solace broker");
            connectionState.set("DISCONNECTING");
            try {
                session.closeSession();
            } catch (Exception e) {
                log.warn("Error during disconnect: {}", sanitize(e.getMessage()));
            }
        }
        connectionState.set("DISCONNECTED");
        connectedSince.set(null);
    }

    public JCSMPSession getSession() {
        JCSMPSession session = sessionRef.get();
        if (session == null || session.isClosed()) {
            throw new ConnectivityException("Not connected to Solace broker");
        }
        return session;
    }

    public boolean isConnected() {
        JCSMPSession session = sessionRef.get();
        return session != null && !session.isClosed();
    }

    public void recordSuccessfulInteraction() {
        lastSuccessfulInteraction.set(Instant.now());
    }

    public String getConnectionState() {
        return connectionState.get();
    }

    public long getReconnectCount() {
        return reconnectCount.get();
    }

    public Instant getConnectedSince() {
        return connectedSince.get();
    }

    public Instant getLastSuccessfulInteraction() {
        return lastSuccessfulInteraction.get();
    }

    public long getUptimeSeconds() {
        Instant since = connectedSince.get();
        if (since == null) return 0;
        return Instant.now().getEpochSecond() - since.getEpochSecond();
    }

    public String getBrokerHostSummary() {
        // Redact credentials from host string if present
        String host = brokerConfig.host();
        if (host != null && host.contains("@")) {
            host = host.replaceAll("://[^@]+@", "://***@");
        }
        return host;
    }

    public String getVpn() {
        return brokerConfig.vpn();
    }

    public boolean isTlsEnabled() {
        return brokerConfig.tlsEnabled();
    }

    @Override
    public void handleEvent(SessionEventArgs event) {
        if (event == null) return;

        SessionEvent eventType = event.getEvent();
        log.info("Solace session event: {} - {}", eventType, sanitize(event.getInfo()));

        switch (eventType) {
            case RECONNECTING -> {
                connectionState.set("RECONNECTING");
                metrics.recordReconnectAttempt();
                reconnectCount.incrementAndGet();
            }
            case RECONNECTED -> {
                connectionState.set("CONNECTED");
                lastSuccessfulInteraction.set(Instant.now());
                log.info("Reconnected to Solace broker");
                if (onReconnectCallback != null) {
                    try {
                        onReconnectCallback.run();
                    } catch (Exception e) {
                        log.error("Error in reconnect callback: {}", sanitize(e.getMessage()));
                    }
                }
            }
            case DOWN_ERROR -> {
                connectionState.set("DOWN");
                log.error("Solace session DOWN: {}", sanitize(event.getInfo()));
            }
            default -> log.debug("Session event: {}", eventType);
        }
    }

    private static String sanitize(String message) {
        if (message == null) return "";
        // Strip potential credential fragments from error messages
        return message.replaceAll("(?i)(password|passwd|secret|token)\\s*[=:]\\s*\\S+", "$1=***");
    }
}
