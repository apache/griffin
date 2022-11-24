package org.apache.griffin.core.worker.entity.enums;

public enum DQEngineEnum {
    PRESTO(0, 1),
    SPARK(1, 2),
    HIVE(2, -1);

    private final int code;
    private final int backEngineCode;

    DQEngineEnum(int code, int backEngineCode) {
        this.code = code;
        this.backEngineCode = backEngineCode;
    }

    public int getCode() {
        return code;
    }

    /**
     * @return DQEngineEnum
     */
    public DQEngineEnum getBackEngine() {
        if (this.backEngineCode == -1) return null;
        return findByCode(this.code);
    }

    public DQEngineEnum findByCode(int code) {
        DQEngineEnum[] values = DQEngineEnum.values();
        for (DQEngineEnum value : values) {
            if (code == value.code) return value;
        }
        return null;
    }
}
