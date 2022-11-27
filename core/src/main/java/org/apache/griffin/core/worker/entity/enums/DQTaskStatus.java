package org.apache.griffin.core.worker.entity.enums;

public enum DQTaskStatus {
    WAITTING(0),
    RECORDING(1),
    RECORDED(1),
    EVALUATING(2),
    EVALUATED(2),
//    ALERTING(3),
//    ALERTED(3),
    SUCCESS(4),
    FAILED(5);

    private final int code;

    DQTaskStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
