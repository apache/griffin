package org.apache.griffin.core.worker.entity.dispatcher;

import lombok.Data;

@Data
public class MetricResponse {
    Integer code;
    Double metric;
//    Enum errorCode;
    Exception ex;
}
