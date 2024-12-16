package org.apache.griffin.metric.exception;

import lombok.Getter;

public enum GriffinErr {
    commonError(101, "Hit an error without details."),
    validationError(102, "Data validation fails due to [%s]"),
    // db operation errors
    dbInsertionError(301, "Fail to insert a record."),
    dbUpdateError(302, "Fail to update a record."),
    dbDeletionError(303, "Fail to delete a record."),
    ;

    @Getter
    private int code;

    @Getter
    private String message;

    GriffinErr(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public GriffinErrorEntity buildErrorEntity() {
        return new GriffinErrorEntity(this);
    }

    public GriffinErrorEntity buildErrorEntity(String message) {
        return new GriffinErrorEntity(this, message);
    }

}
