package com.solace.mcp.error;

/**
 * Machine-friendly error codes for MCP tool responses.
 */
public enum McpErrorCode {
    CONFIGURATION_ERROR("CONFIGURATION_ERROR"),
    AUTHENTICATION_ERROR("AUTHENTICATION_ERROR"),
    AUTHORIZATION_ERROR("AUTHORIZATION_ERROR"),
    TOPIC_POLICY_VIOLATION("TOPIC_POLICY_VIOLATION"),
    CONNECTIVITY_ERROR("CONNECTIVITY_ERROR"),
    REQUEST_TIMEOUT("REQUEST_TIMEOUT"),
    PAYLOAD_VALIDATION_ERROR("PAYLOAD_VALIDATION_ERROR"),
    SUBSCRIPTION_ERROR("SUBSCRIPTION_ERROR"),
    INTERNAL_ERROR("INTERNAL_ERROR");

    private final String code;

    McpErrorCode(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}
