package org.apache.griffin.metric.exception;

import lombok.Getter;

public class GriffinException extends RuntimeException {
    @Getter
    private final GriffinErr error;

    public GriffinException(String message) {
        super(message);
        this.error = GriffinErr.commonError;
    }

    public GriffinException(Throwable cause) {
        super(cause);
        this.error = GriffinErr.commonError;
    }

    public GriffinException(String message, Throwable cause) {
        super(message, cause);
        this.error = GriffinErr.commonError;
    }

    public GriffinException(String message, GriffinErr error) {
        super(message);
        this.error = error;
    }
}
