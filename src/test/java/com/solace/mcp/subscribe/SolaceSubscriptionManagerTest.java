package com.solace.mcp.subscribe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.*;

class SolaceSubscriptionManagerTest {

    @ParameterizedTest
    @CsvSource({
            "orders/new,         orders/new,         true",
            "orders/new,         orders/*,           true",
            "orders/new/detail,  orders/>,           true",
            "orders/new,         orders/>,           true",
            "orders,             >,                  true",
            "a/b/c,              a/*/c,              true",
            "a/b/c,              a/b/*,              true",
            "a/b/c,              a/>,                true",
            "a/b/c,              */b/c,              true",
            "orders/new,         marketing/*,        false",
            "orders/new/detail,  orders/*,           false",
            "a/b,                a/b/c,              false",
    })
    void topicMatchesSubscription(String topic, String pattern, boolean expected) {
        assertThat(SolaceSubscriptionManager.topicMatchesSubscription(topic, pattern.trim()))
                .isEqualTo(expected);
    }

    @Test
    void exactMatchWorks() {
        assertThat(SolaceSubscriptionManager.topicMatchesSubscription("a/b/c", "a/b/c")).isTrue();
    }

    @Test
    void emptyTopicDoesNotMatch() {
        assertThat(SolaceSubscriptionManager.topicMatchesSubscription("", "a/*")).isFalse();
    }

    @Test
    void resourceUriFormat() {
        assertThat(SolaceSubscriptionManager.resourceUri("sub-123"))
                .isEqualTo("solace://subscriptions/sub-123/messages");
    }

    @Test
    void resourceUriPrefixAndSuffixConsistent() {
        String uri = SolaceSubscriptionManager.resourceUri("abc");
        assertThat(uri).startsWith(SolaceSubscriptionManager.RESOURCE_URI_PREFIX);
        assertThat(uri).endsWith(SolaceSubscriptionManager.RESOURCE_URI_SUFFIX);
    }
}
