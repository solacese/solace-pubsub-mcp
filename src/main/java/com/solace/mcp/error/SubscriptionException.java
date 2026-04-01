package com.solace.mcp.error;

public class SubscriptionException extends SolaceMcpException {
    public SubscriptionException(String message) {
        super(McpErrorCode.SUBSCRIPTION_ERROR, message);
    }

    public SubscriptionException(String message, Throwable cause) {
        super(McpErrorCode.SUBSCRIPTION_ERROR, message, cause);
    }
}
