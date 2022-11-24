package org.apache.griffin.core.worker.entity.enums;

public enum DQErrorCode {
    SUCCESS(200),
    INTERNAL_ERROR(500),
    EXTERNAL_ERROR(400);

    private final int code;

    DQErrorCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
