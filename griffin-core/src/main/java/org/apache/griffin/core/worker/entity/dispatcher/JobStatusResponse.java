package org.apache.griffin.core.worker.entity.dispatcher;

import org.apache.griffin.core.worker.entity.enums.DQErrorCode;
import org.apache.griffin.core.worker.entity.enums.DispatcherJobStatusEnum;

public class JobStatusResponse {
    private Integer code;
    private DispatcherJobStatusEnum jobStatus;
    private DQErrorCode errorCode;
    private Exception ex;
    public boolean isSuccess() {
        return false;
    }
}
