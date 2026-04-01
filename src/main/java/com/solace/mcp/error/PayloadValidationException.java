package com.solace.mcp.error;

public class PayloadValidationException extends SolaceMcpException {
    public PayloadValidationException(String message) {
        super(McpErrorCode.PAYLOAD_VALIDATION_ERROR, message);
    }
}
