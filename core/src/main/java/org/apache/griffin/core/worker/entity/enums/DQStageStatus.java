package org.apache.griffin.core.worker.entity.enums;

public enum DQStageStatus {
    INIT(0),
    RUNNABLE(1),
    RUNNING(2),
    FINISH(3);

    private final int code;

    DQStageStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
