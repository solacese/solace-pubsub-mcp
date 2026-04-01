package com.solace.mcp.config;

import com.solace.mcp.error.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Loads configuration from environment variables with safe defaults.
 * Validates all values at startup and fails fast on invalid config.
 */
public final class ConfigurationLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigurationLoader.class);

    private ConfigurationLoader() {}

    public static SolaceMcpConfig load() {
        log.info("Loading Solace PubSub MCP configuration from environment");

        var broker = loadBrokerConfig();
        var limits = loadLimitsConfig();
        var topicPolicy = loadTopicPolicyConfig();
        var observability = loadObservabilityConfig();

        var config = new SolaceMcpConfig(broker, limits, topicPolicy, observability);
        validate(config);

        log.info("Configuration loaded: {}", broker.safeDescription());
        return config;
    }

    private static SolaceMcpConfig.BrokerConfig loadBrokerConfig() {
        return new SolaceMcpConfig.BrokerConfig(
                requireEnv("SOLACE_HOST", "tcp://localhost:55555"),
                env("SOLACE_VPN", "default"),
                env("SOLACE_USERNAME", "default"),
                env("SOLACE_PASSWORD", "default"),
                envBool("SOLACE_TLS_ENABLED", false),
                env("SOLACE_TLS_TRUST_STORE_PATH", null),
                env("SOLACE_TLS_TRUST_STORE_PASSWORD", null),
                new SolaceMcpConfig.ReconnectConfig(
                        envBool("SOLACE_RECONNECT_ENABLED", true),
                        envInt("SOLACE_RECONNECT_RETRIES", -1),
                        envLong("SOLACE_RECONNECT_INITIAL_DELAY_MS", 1000),
                        envLong("SOLACE_RECONNECT_MAX_DELAY_MS", 30000)
                )
        );
    }

    private static SolaceMcpConfig.LimitsConfig loadLimitsConfig() {
        return new SolaceMcpConfig.LimitsConfig(
                envInt("SOLACE_MAX_PAYLOAD_BYTES", 1_048_576),
                envInt("SOLACE_MAX_ACTIVE_SUBSCRIPTIONS", 50),
                envInt("SOLACE_MAX_BUFFER_PER_SUBSCRIPTION", 1000),
                envLong("SOLACE_REQUEST_TIMEOUT_MS", 30_000)
        );
    }

    private static SolaceMcpConfig.TopicPolicyConfig loadTopicPolicyConfig() {
        return new SolaceMcpConfig.TopicPolicyConfig(
                envList("SOLACE_TOPIC_ALLOW_LIST"),
                envList("SOLACE_TOPIC_DENY_LIST"),
                envBool("SOLACE_ALLOW_WILDCARD_SUBSCRIPTIONS", true),
                envBool("SOLACE_ALLOW_FULL_WILDCARD", false)
        );
    }

    private static SolaceMcpConfig.ObservabilityConfig loadObservabilityConfig() {
        return new SolaceMcpConfig.ObservabilityConfig(
                env("SOLACE_MCP_LOG_LEVEL", "INFO"),
                envBool("SOLACE_METRICS_ENABLED", true)
        );
    }

    private static void validate(SolaceMcpConfig config) {
        if (config.broker().host() == null || config.broker().host().isBlank()) {
            throw new ConfigurationException("SOLACE_HOST is required");
        }
        if (config.limits().maxPayloadBytes() <= 0) {
            throw new ConfigurationException("SOLACE_MAX_PAYLOAD_BYTES must be positive");
        }
        if (config.limits().maxActiveSubscriptions() <= 0) {
            throw new ConfigurationException("SOLACE_MAX_ACTIVE_SUBSCRIPTIONS must be positive");
        }
        if (config.limits().maxBufferPerSubscription() <= 0) {
            throw new ConfigurationException("SOLACE_MAX_BUFFER_PER_SUBSCRIPTION must be positive");
        }
        if (config.limits().requestTimeoutMs() <= 0) {
            throw new ConfigurationException("SOLACE_REQUEST_TIMEOUT_MS must be positive");
        }
        if (config.broker().tlsEnabled() && config.broker().host() != null
                && !config.broker().host().startsWith("tcps://")) {
            log.warn("TLS is enabled but host does not use tcps:// scheme: {}", config.broker().host());
        }
    }

    private static String requireEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value.trim() : defaultValue;
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value.trim() : defaultValue;
    }

    private static boolean envBool(String key, boolean defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) return defaultValue;
        return Boolean.parseBoolean(value.trim());
    }

    private static int envInt(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new ConfigurationException("Invalid integer for %s: %s".formatted(key, value));
        }
    }

    private static long envLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new ConfigurationException("Invalid long for %s: %s".formatted(key, value));
        }
    }

    private static List<String> envList(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) return List.of();
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}
