package org.apache.griffin.core.worker.client;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.dispatcher.*;
import org.apache.griffin.core.worker.entity.enums.DQErrorCode;
import org.springframework.stereotype.Service;

/**
 *
 */
@Service
public class DispatcherClient {

    public SubmitResponse submitSql(SubmitRequest request) {
        return null;
    }


    public JobStatusResponse getJobStatus(JobStatusRequest jobStatusRequest) {
        // todo parse job status
        return null;
    }

    public MetricResponse getMetricResult(MetricRequest metricRequest) {
        return null;
    }

    public SubmitRequest wrapperDQTask(DQInstance waittingTask) {
        return null;
    }

    public JobStatus wrapperSubmitResponse(Pair<Long, SubmitResponse> resp) {
        Long partitionTime = resp.getLeft();
        SubmitResponse response = resp.getRight();
        boolean finished = false;
        if (response.getCode() != DQErrorCode.SUCCESS.getCode()) finished = true;
        return JobStatus.builder()
                .jobId(response.getJobId())
                .finished(finished)
                .partitionTime(partitionTime)
                .build();
    }

    public JobStatusRequest wrapperJobStatusRequest(JobStatus jobStatus) {
        return new JobStatusRequest(jobStatus.getJobId());
    }

    public MetricRequest wrapperMetricRequest(JobStatus jobStatus) {
        return new MetricRequest(jobStatus.getJobId());
    }
}
