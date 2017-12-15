package org.apache.griffin.core.job.entity;

import javax.persistence.Entity;

@Entity
public class VirtualJob extends Job {

    public VirtualJob() {
        super();
    }

    public VirtualJob(String jobName, Long measureId, String metricName) {
        super(jobName, measureId, metricName);
    }
}
