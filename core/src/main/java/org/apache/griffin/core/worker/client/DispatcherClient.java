package org.apache.griffin.core.worker.client;

import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.dispatcher.*;

/**
 * Dispatcher客户端 负责与dispatcher交互
 */
public class DispatcherClient {

    public SubmitResponse submitSql(SubmitRequest request) {
        return null;
    }


    public JobStatusResponse getJobStatus(JobStatusRequest jobStatusRequest) {
        return null;
    }

    public double getMetricResult() {
        return 0.0d;
    }

    public SubmitRequest wrapperDQTask(DQInstance waittingTask) {
        return null;
    }

    public JobStatus wrapperSubmitResponse(SubmitResponse resp) {
        return null;
    }

    public JobStatusRequest wrapperJobStatusRequest(JobStatus jobStatus) {
        return null;
    }
}
