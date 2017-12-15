package org.apache.griffin.core.job.entity;

import javax.persistence.Entity;

@Entity
public class GriffinJob extends Job {

    private String groupName;
    private String quartzJobName;
    private String quartzGroupName;

    public GriffinJob() {
        super();
    }

    public GriffinJob(String jobName, Long measureId, String metricName, String groupName, String quartzJobName, String quartzGroupName) {
        super(jobName, measureId, metricName);
        this.groupName = groupName;
        this.quartzJobName = quartzJobName;
        this.quartzGroupName = quartzGroupName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getQuartzJobName() {
        return quartzJobName;
    }

    public void setQuartzJobName(String quartzJobName) {
        this.quartzJobName = quartzJobName;
    }

    public String getQuartzGroupName() {
        return quartzGroupName;
    }

    public void setQuartzGroupName(String quartzGroupName) {
        this.quartzGroupName = quartzGroupName;
    }
}
