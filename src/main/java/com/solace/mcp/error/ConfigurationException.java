package com.solace.mcp.error;

public class ConfigurationException extends SolaceMcpException {
    public ConfigurationException(String message) {
        super(McpErrorCode.CONFIGURATION_ERROR, message);
    }
}
