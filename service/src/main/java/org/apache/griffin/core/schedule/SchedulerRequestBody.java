package org.apache.griffin.core.schedule;

/**
 * Created by xiangrchen on 4/27/17.
 */
public class SchedulerRequestBody {
    String sourcePat;
    String targetPat;
    String dataStartTimestamp;
    String jobStartTime;
    String periodTime;

    public String getSourcePat() {
        return sourcePat;
    }

    public void setSourcePat(String sourcePat) {
        this.sourcePat = sourcePat;
    }

    public String getTargetPat() {
        return targetPat;
    }

    public void setTargetPat(String targetPat) {
        this.targetPat = targetPat;
    }

    public String getDataStartTimestamp() {
        return dataStartTimestamp;
    }

    public void setDataStartTimestamp(String dataStartTimestamp) {
        this.dataStartTimestamp = dataStartTimestamp;
    }

    public String getJobStartTime() {
        return jobStartTime;
    }

    public void setJobStartTime(String jobStartTime) {
        this.jobStartTime = jobStartTime;
    }

    public String getPeriodTime() {
        return periodTime;
    }

    public void setPeriodTime(String periodTime) {
        this.periodTime = periodTime;
    }

}
