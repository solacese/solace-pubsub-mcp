package com.solace.mcp.error;

/**
 * Base exception for all Solace PubSub MCP errors.
 * Carries a machine-friendly error code and a safe message (no secrets).
 */
public class SolaceMcpException extends RuntimeException {

    private final McpErrorCode errorCode;

    public SolaceMcpException(McpErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public SolaceMcpException(McpErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public McpErrorCode errorCode() {
        return errorCode;
    }
}
