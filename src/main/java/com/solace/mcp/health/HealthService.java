package com.solace.mcp.health;

import com.solace.mcp.connection.BrokerConnectionManager;
import com.solace.mcp.metrics.MetricsService;
import com.solace.mcp.model.ConnectionStatus;
import com.solace.mcp.model.HealthStatus;
import com.solace.mcp.subscribe.SolaceSubscriptionManager;

/**
 * Provides health and connection status reporting.
 */
public class HealthService {

    private final BrokerConnectionManager connectionManager;
    private final SolaceSubscriptionManager subscriptionManager;
    private final MetricsService metrics;

    public HealthService(
            BrokerConnectionManager connectionManager,
            SolaceSubscriptionManager subscriptionManager,
            MetricsService metrics) {
        this.connectionManager = connectionManager;
        this.subscriptionManager = subscriptionManager;
        this.metrics = metrics;
    }

    public ConnectionStatus getConnectionStatus() {
        return new ConnectionStatus(
                connectionManager.isConnected(),
                connectionManager.getBrokerHostSummary(),
                connectionManager.getVpn(),
                connectionManager.isTlsEnabled(),
                connectionManager.getConnectionState(),
                connectionManager.getUptimeSeconds(),
                connectionManager.getReconnectCount(),
                subscriptionManager.getActiveCount()
        );
    }

    public HealthStatus getHealth() {
        boolean connected = connectionManager.isConnected();
        String status = connected ? "UP" : "DOWN";
        String reconnectState = connectionManager.getConnectionState();

        return new HealthStatus(
                status,
                connected,
                connectionManager.getLastSuccessfulInteraction(),
                reconnectState,
                metrics.snapshot()
        );
    }
}
