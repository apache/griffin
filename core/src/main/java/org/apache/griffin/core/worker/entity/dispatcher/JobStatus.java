package org.apache.griffin.core.worker.entity.dispatcher;

import lombok.Data;

@Data
public class JobStatus {
    private String jobId;
    private boolean finished = false;

}
