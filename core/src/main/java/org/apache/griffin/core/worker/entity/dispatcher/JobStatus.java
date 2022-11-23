package org.apache.griffin.core.worker.entity.dispatcher;

import lombok.Data;

@Data
public class JobStatus {
    private String jobId;
    private boolean finished = false;
    private long partitionTime;

    public JobStatus() {
    }

    public JobStatus(String jobId, boolean finished, long partitionTime) {
        this.jobId = jobId;
        this.finished = finished;
        this.partitionTime = partitionTime;
    }

    public static Builder builder() {
        return new JobStatus.Builder();
    }

    public static class Builder {
        private String jobId;
        private boolean finished = false;
        private long partitionTime;

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder finished(boolean finished) {
            this.finished = finished;
            return this;
        }

        public Builder partitionTime(long partitionTime) {
            this.partitionTime = partitionTime;
            return this;
        }

        public JobStatus build() {
            return new JobStatus(this.jobId, this.finished, this.partitionTime);
        }
    }
}
