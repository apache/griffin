package org.apache.griffin.api.common;

public enum GRPCCode {
    SUCCESS(200),
    CLIENT_ERROR(500),
    SERVER_ERROR(500),
    EXTERNAL_ERROR(400);

    private final int code;

    GRPCCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
