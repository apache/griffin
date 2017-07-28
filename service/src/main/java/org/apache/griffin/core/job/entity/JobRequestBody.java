/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.core.job.entity;

/**
 * Created by xiangrchen on 4/27/17.
 */
public class JobRequestBody {
    private String sourcePat;
    private String targetPat;
    private String dataStartTimestamp;
    private String jobStartTime;
    private String periodTime;

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

    public JobRequestBody(String sourcePat, String targetPat, String dataStartTimestamp, String jobStartTime, String periodTime) {
        this.sourcePat = sourcePat;
        this.targetPat = targetPat;
        this.dataStartTimestamp = dataStartTimestamp;
        this.jobStartTime = jobStartTime;
        this.periodTime = periodTime;
    }

    public JobRequestBody() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobRequestBody that = (JobRequestBody) o;

        if (sourcePat != null ? !sourcePat.equals(that.sourcePat) : that.sourcePat != null) return false;
        if (targetPat != null ? !targetPat.equals(that.targetPat) : that.targetPat != null) return false;
        if (dataStartTimestamp != null ? !dataStartTimestamp.equals(that.dataStartTimestamp) : that.dataStartTimestamp != null)
            return false;
        if (jobStartTime != null ? !jobStartTime.equals(that.jobStartTime) : that.jobStartTime != null) return false;
        return periodTime != null ? periodTime.equals(that.periodTime) : that.periodTime == null;
    }

    @Override
    public int hashCode() {
        int result = sourcePat != null ? sourcePat.hashCode() : 0;
        result = 31 * result + (targetPat != null ? targetPat.hashCode() : 0);
        result = 31 * result + (dataStartTimestamp != null ? dataStartTimestamp.hashCode() : 0);
        result = 31 * result + (jobStartTime != null ? jobStartTime.hashCode() : 0);
        result = 31 * result + (periodTime != null ? periodTime.hashCode() : 0);
        return result;
    }
}
