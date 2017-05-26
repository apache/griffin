package org.apache.griffin.core.util;


public enum GriffinOperationMessage {
    RESOURCE_NOT_FOUND(-1, "Resource Not Found"),
    DELETE_MEASURE_BY_NAME_SUCCESS(0, "Delete Measures By Name Succeed"),
    DELETE_MEASURE_BY_NAME_FAIL(1, "Delete Measures By Name Failed"),
    UPDATE_MEASURE_SUCCESS(2, "Update Measure Succeed"),
    UPDATE_MEASURE_FAIL(3, "Update Measure Failed"),
    CREATE_MEASURE_SUCCESS(4, "Create Measure Succeed"),
    CREATE_MEASURE_FAIL(5, "Create Measure Failed"),
    CREATE_MEASURE_FAIL_DUPLICATE(6, "Create Measure Failed, duplicate records"),
    ;

    private final int code;
    private final String description;

    private GriffinOperationMessage(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return code + ": " + description;
    }
}
