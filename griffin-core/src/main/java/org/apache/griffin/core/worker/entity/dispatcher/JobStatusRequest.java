package org.apache.griffin.core.worker.entity.dispatcher;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JobStatusRequest {
    String jobId;
}
