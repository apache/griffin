package org.apache.griffin.common.exception;

public class JobException extends BaseException {
    public JobException(int code) {
        this(code, null, null);
    }

    public JobException(int code, Throwable cause) {
        this(code, null, cause);
    }

    public JobException(int code, String message, Throwable cause) {
        super(code, message, cause);
    }
    public JobException(int code, String message, Throwable cause,
                        boolean enableSuppression, boolean writableStackTrace) {
        super(code, message, cause, enableSuppression, writableStackTrace);
    }

}
