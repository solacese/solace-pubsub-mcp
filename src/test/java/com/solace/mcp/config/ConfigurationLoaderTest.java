package com.solace.mcp.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class ConfigurationLoaderTest {

    @Test
    void loadsDefaultsWhenNoEnvVarsSet() {
        // This test verifies defaults are reasonable without needing actual env vars.
        // ConfigurationLoader.load() reads from System.getenv, so defaults apply.
        SolaceMcpConfig config = ConfigurationLoader.load();

        assertThat(config.broker().host()).isEqualTo("tcp://localhost:55555");
        assertThat(config.broker().vpn()).isEqualTo("default");
        assertThat(config.broker().username()).isEqualTo("default");
        assertThat(config.broker().tlsEnabled()).isFalse();
        assertThat(config.broker().reconnect().enabled()).isTrue();
        assertThat(config.limits().maxPayloadBytes()).isEqualTo(1_048_576);
        assertThat(config.limits().maxActiveSubscriptions()).isEqualTo(50);
        assertThat(config.limits().maxBufferPerSubscription()).isEqualTo(1000);
        assertThat(config.limits().requestTimeoutMs()).isEqualTo(30_000);
        assertThat(config.topicPolicy().allowList()).isEmpty();
        assertThat(config.topicPolicy().denyList()).isEmpty();
        assertThat(config.topicPolicy().allowWildcardSubscriptions()).isTrue();
        assertThat(config.topicPolicy().allowFullWildcard()).isFalse();
        assertThat(config.observability().metricsEnabled()).isTrue();
    }

    @Test
    void brokerConfigSafeDescriptionRedactsPassword() {
        var broker = new SolaceMcpConfig.BrokerConfig(
                "tcps://broker.example.com:55443", "prod-vpn", "svc-account",
                "super-secret", true, null, null,
                new SolaceMcpConfig.ReconnectConfig(true, -1, 1000, 30000));

        String description = broker.safeDescription();
        assertThat(description).contains("broker.example.com");
        assertThat(description).contains("svc-account");
        assertThat(description).doesNotContain("super-secret");
    }
}
