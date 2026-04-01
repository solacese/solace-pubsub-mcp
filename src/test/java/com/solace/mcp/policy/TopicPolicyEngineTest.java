package com.solace.mcp.policy;

import com.solace.mcp.config.SolaceMcpConfig;
import com.solace.mcp.error.TopicPolicyViolationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class TopicPolicyEngineTest {

    @Test
    void publishToValidTopic() {
        var engine = engine(List.of(), List.of(), true, false);
        assertThatCode(() -> engine.validateForPublish("orders/new"))
                .doesNotThrowAnyException();
    }

    @Test
    void publishRejectsWildcardTopic() {
        var engine = engine(List.of(), List.of(), true, true);
        assertThatThrownBy(() -> engine.validateForPublish("orders/*"))
                .isInstanceOf(TopicPolicyViolationException.class)
                .hasMessageContaining("wildcards");
    }

    @Test
    void publishRejectsEmptyTopic() {
        var engine = engine(List.of(), List.of(), true, false);
        assertThatThrownBy(() -> engine.validateForPublish(""))
                .isInstanceOf(TopicPolicyViolationException.class);
    }

    @Test
    void publishRejectsNullTopic() {
        var engine = engine(List.of(), List.of(), true, false);
        assertThatThrownBy(() -> engine.validateForPublish(null))
                .isInstanceOf(TopicPolicyViolationException.class);
    }

    @Test
    void publishRejectsLongTopic() {
        var engine = engine(List.of(), List.of(), true, false);
        String longTopic = "a".repeat(251);
        assertThatThrownBy(() -> engine.validateForPublish(longTopic))
                .isInstanceOf(TopicPolicyViolationException.class)
                .hasMessageContaining("maximum length");
    }

    @Test
    void allowListPermitsTopic() {
        var engine = engine(List.of("finance/orders/*"), List.of(), true, false);
        assertThatCode(() -> engine.validateForPublish("finance/orders/new"))
                .doesNotThrowAnyException();
    }

    @Test
    void allowListRejectsTopic() {
        var engine = engine(List.of("finance/orders/*"), List.of(), true, false);
        assertThatThrownBy(() -> engine.validateForPublish("marketing/campaigns/new"))
                .isInstanceOf(TopicPolicyViolationException.class)
                .hasMessageContaining("not in the allow list");
    }

    @Test
    void denyListOverridesAllowList() {
        var engine = engine(
                List.of("finance/>"),
                List.of("finance/orders/admin/*"),
                true, false);
        // Allowed by allow list but denied by deny list
        assertThatThrownBy(() -> engine.validateForPublish("finance/orders/admin/delete"))
                .isInstanceOf(TopicPolicyViolationException.class)
                .hasMessageContaining("denied");
    }

    @Test
    void denyListAllowsNonMatchingTopic() {
        var engine = engine(
                List.of("finance/>"),
                List.of("finance/orders/admin/*"),
                true, false);
        assertThatCode(() -> engine.validateForPublish("finance/orders/new"))
                .doesNotThrowAnyException();
    }

    @Test
    void subscribeWithWildcardAllowed() {
        var engine = engine(List.of(), List.of(), true, false);
        assertThatCode(() -> engine.validateForSubscribe("orders/*"))
                .doesNotThrowAnyException();
    }

    @Test
    void subscribeWithWildcardDisabled() {
        var engine = engine(List.of(), List.of(), false, false);
        assertThatThrownBy(() -> engine.validateForSubscribe("orders/*"))
                .isInstanceOf(TopicPolicyViolationException.class)
                .hasMessageContaining("Wildcard subscriptions are disabled");
    }

    @Test
    void subscribeFullWildcardRejectedByDefault() {
        var engine = engine(List.of(), List.of(), true, false);
        assertThatThrownBy(() -> engine.validateForSubscribe(">"))
                .isInstanceOf(TopicPolicyViolationException.class)
                .hasMessageContaining("Full-tree wildcard");
    }

    @Test
    void subscribeFullWildcardAllowedWhenEnabled() {
        var engine = engine(List.of(), List.of(), true, true);
        assertThatCode(() -> engine.validateForSubscribe(">"))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({
            "orders/new,         orders/new,         true",
            "orders/new,         orders/*,           true",
            "orders/new/detail,  orders/>,           true",
            "orders/new,         orders/>,           true",
            "orders/new,         marketing/*,        false",
            "orders/new/detail,  orders/*,           false",
            "a/b/c,              a/*/c,              true",
            "a/b/c,              a/>  ,              true",
            "a,                  >,                  true",
    })
    void topicMatchesPattern(String topic, String pattern, boolean expected) {
        assertThat(TopicPolicyEngine.topicMatchesPattern(topic, pattern.trim())).isEqualTo(expected);
    }

    private static TopicPolicyEngine engine(
            List<String> allow, List<String> deny, boolean wildcards, boolean fullWildcard) {
        return new TopicPolicyEngine(new SolaceMcpConfig.TopicPolicyConfig(
                allow, deny, wildcards, fullWildcard));
    }
}
