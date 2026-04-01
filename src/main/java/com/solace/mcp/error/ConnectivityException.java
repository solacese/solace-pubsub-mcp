package com.solace.mcp.error;

public class ConnectivityException extends SolaceMcpException {
    public ConnectivityException(String message) {
        super(McpErrorCode.CONNECTIVITY_ERROR, message);
    }

    public ConnectivityException(String message, Throwable cause) {
        super(McpErrorCode.CONNECTIVITY_ERROR, message, cause);
    }
}
