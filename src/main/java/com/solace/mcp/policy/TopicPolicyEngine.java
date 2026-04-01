package com.solace.mcp.policy;

import com.solace.mcp.config.SolaceMcpConfig;
import com.solace.mcp.error.TopicPolicyViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Enforces topic allow/deny rules and wildcard policy.
 * Deny list overrides allow list. Evaluated before every publish/subscribe.
 *
 * Solace wildcard characters:
 *   * matches one level
 *   > matches one or more trailing levels
 */
public final class TopicPolicyEngine {

    private static final Logger log = LoggerFactory.getLogger(TopicPolicyEngine.class);

    private final List<String> allowList;
    private final List<String> denyList;
    private final boolean allowWildcardSubscriptions;
    private final boolean allowFullWildcard;

    public TopicPolicyEngine(SolaceMcpConfig.TopicPolicyConfig policyConfig) {
        this.allowList = policyConfig.allowList();
        this.denyList = policyConfig.denyList();
        this.allowWildcardSubscriptions = policyConfig.allowWildcardSubscriptions();
        this.allowFullWildcard = policyConfig.allowFullWildcard();
    }

    /**
     * Validates a topic for publishing. Publishes never use wildcards.
     */
    public void validateForPublish(String topic) {
        validateNotEmpty(topic);
        if (containsWildcard(topic)) {
            throw new TopicPolicyViolationException("Publish topics must not contain wildcards: " + topic);
        }
        checkDenyList(topic);
        checkAllowList(topic);
    }

    /**
     * Validates a topic for subscribing. May contain wildcards if policy permits.
     */
    public void validateForSubscribe(String topic) {
        validateNotEmpty(topic);
        checkWildcardPolicy(topic);
        checkDenyList(topic);
        checkAllowList(topic);
    }

    private void validateNotEmpty(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new TopicPolicyViolationException("Topic must not be empty");
        }
        if (topic.length() > 250) {
            throw new TopicPolicyViolationException("Topic exceeds maximum length of 250 characters");
        }
    }

    private void checkWildcardPolicy(String topic) {
        if (!containsWildcard(topic)) {
            return;
        }
        if (!allowWildcardSubscriptions) {
            throw new TopicPolicyViolationException(
                    "Wildcard subscriptions are disabled by policy: " + topic);
        }
        // ">" alone = full tree wildcard
        if (isFullWildcard(topic) && !allowFullWildcard) {
            throw new TopicPolicyViolationException(
                    "Full-tree wildcard subscriptions (>) are disabled by policy: " + topic);
        }
    }

    private void checkDenyList(String topic) {
        if (denyList.isEmpty()) return;
        for (String pattern : denyList) {
            if (topicMatchesPattern(topic, pattern)) {
                log.debug("Topic '{}' denied by pattern '{}'", topic, pattern);
                throw new TopicPolicyViolationException(
                        "Topic '%s' is denied by policy pattern '%s'".formatted(topic, pattern));
            }
        }
    }

    private void checkAllowList(String topic) {
        if (allowList.isEmpty()) return; // empty allow list = allow all (not denied)
        for (String pattern : allowList) {
            if (topicMatchesPattern(topic, pattern)) {
                return; // allowed
            }
        }
        throw new TopicPolicyViolationException(
                "Topic '%s' is not in the allow list".formatted(topic));
    }

    /**
     * Matches a concrete topic or subscription pattern against a policy pattern.
     * Uses Solace-style matching:
     *   - '/' is the level separator
     *   - '*' matches exactly one level
     *   - '>' matches one or more trailing levels
     *   - Literal segments must match exactly
     */
    static boolean topicMatchesPattern(String topic, String pattern) {
        String[] topicParts = topic.split("/", -1);
        String[] patternParts = pattern.split("/", -1);

        for (int i = 0; i < patternParts.length; i++) {
            String pp = patternParts[i];

            if (pp.equals(">")) {
                // > matches rest of topic if at least one level remains
                return i < topicParts.length;
            }
            if (i >= topicParts.length) {
                return false;
            }
            if (pp.equals("*")) {
                continue; // matches any single level
            }
            if (!pp.equals(topicParts[i])) {
                return false;
            }
        }
        return topicParts.length == patternParts.length;
    }

    private static boolean containsWildcard(String topic) {
        return topic.contains("*") || topic.contains(">");
    }

    private static boolean isFullWildcard(String topic) {
        String trimmed = topic.trim();
        return trimmed.equals(">")
                || trimmed.endsWith("/>") && trimmed.chars().filter(c -> c == '/').count() <= 1;
    }
}
