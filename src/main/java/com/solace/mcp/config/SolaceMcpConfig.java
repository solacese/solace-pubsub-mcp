package com.solace.mcp.config;

import java.util.List;

/**
 * Strongly-typed configuration for Solace PubSub MCP.
 * Loaded from environment variables with safe defaults.
 */
public record SolaceMcpConfig(
        BrokerConfig broker,
        LimitsConfig limits,
        TopicPolicyConfig topicPolicy,
        ObservabilityConfig observability
) {

    public record BrokerConfig(
            String host,
            String vpn,
            String username,
            String password,
            boolean tlsEnabled,
            String tlsTrustStorePath,
            String tlsTrustStorePassword,
            ReconnectConfig reconnect
    ) {
        /** Returns host with password redacted for logging. */
        public String safeDescription() {
            return "host=%s vpn=%s user=%s tls=%s".formatted(host, vpn, username, tlsEnabled);
        }
    }

    public record ReconnectConfig(
            boolean enabled,
            int retries,
            long initialDelayMs,
            long maxDelayMs
    ) {}

    public record LimitsConfig(
            int maxPayloadBytes,
            int maxActiveSubscriptions,
            int maxBufferPerSubscription,
            long requestTimeoutMs
    ) {}

    public record TopicPolicyConfig(
            List<String> allowList,
            List<String> denyList,
            boolean allowWildcardSubscriptions,
            boolean allowFullWildcard
    ) {}

    public record ObservabilityConfig(
            String logLevel,
            boolean metricsEnabled
    ) {}
}
