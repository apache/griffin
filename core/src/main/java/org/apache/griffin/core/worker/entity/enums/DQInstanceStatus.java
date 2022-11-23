package org.apache.griffin.core.worker.entity.enums;

public enum DQInstanceStatus {
    ACCEPTED(0),
    WAITTING(1),
    SUBMITTING(2), // 任务提交中
    RUNNING(3),
    RECORDING(4),
    EVALUATING(5),
    EVALUATE_ALERTING(6), // Metric 需要告警
    FAILED_ALERTING(7),   // 任务执行失败需要告警
    SUCCESS(8),
    FAILED(9);

    private final int code;

    DQInstanceStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
