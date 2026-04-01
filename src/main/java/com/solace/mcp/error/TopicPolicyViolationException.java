package com.solace.mcp.error;

public class TopicPolicyViolationException extends SolaceMcpException {
    public TopicPolicyViolationException(String message) {
        super(McpErrorCode.TOPIC_POLICY_VIOLATION, message);
    }
}
