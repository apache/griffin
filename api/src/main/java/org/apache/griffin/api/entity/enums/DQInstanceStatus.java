package org.apache.griffin.api.entity.enums;

public enum DQInstanceStatus {
    ACCEPTED(0),
    WAITTING(1),
//    SUBMITTING(2), // 任务提交中
//    RUNNING(3),
    RECORDING(4),
    EVALUATING(5),
    ALERTING(5),
//    EVALUATE_ALERTING(6), // Metric 需要告警
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

    public static DQInstanceStatus findByCode(int statusCode) throws Exception {
        DQInstanceStatus[] values = DQInstanceStatus.values();
        for (DQInstanceStatus value : values) {
            if (value.code == statusCode) return value;
        }
        throw new Exception("Unknown DQInstanceStatus Code: " + statusCode);
    }
}
