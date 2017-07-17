package org.apache.griffin.core.schedule.entity;

/**
 * Created by xiangrchen on 7/17/17.
 */
public class GroupAndJobName {
    String groupName;
    String jobName;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public GroupAndJobName() {
    }

    public GroupAndJobName(String groupName, String jobName) {
        this.groupName = groupName;
        this.jobName = jobName;
    }
}
