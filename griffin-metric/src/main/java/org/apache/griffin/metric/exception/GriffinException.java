package org.apache.griffin.metric.exception;

public class GriffinException extends RuntimeException {
    public GriffinException(String message) {
        super(message);
    }

    public GriffinException(Throwable cause) {
        super(cause);
    }

    public GriffinException(String message, Throwable cause) {
        super(message, cause);
    }
}
