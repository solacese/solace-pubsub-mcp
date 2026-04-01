package com.solace.mcp.error;

public class RequestTimeoutException extends SolaceMcpException {
    public RequestTimeoutException(String message) {
        super(McpErrorCode.REQUEST_TIMEOUT, message);
    }
}
