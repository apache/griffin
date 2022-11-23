package org.apache.griffin.core.worker.entity.dispatcher;

import lombok.Data;
import org.apache.griffin.core.worker.entity.enums.DQErrorCode;

@Data
public class SubmitResponse {
    private Integer code;
    private String jobId;
    private DQErrorCode errorCode;
    private Exception ex;
}
