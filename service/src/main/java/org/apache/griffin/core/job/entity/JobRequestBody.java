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

public class JobRequestBody {
    private String sourcePattern;
    private String targetPattern;
    private String blockStartTimestamp;
    private String jobStartTime;
    private String interval;

    public String getSourcePattern() {
        return sourcePattern;
    }

    public void setSourcePattern(String sourcePattern) {
        this.sourcePattern = sourcePattern;
    }

    public String getTargetPattern() {
        return targetPattern;
    }

    public void setTargetPattern(String targetPattern) {
        this.targetPattern = targetPattern;
    }

    public String getBlockStartTimestamp() {
        return blockStartTimestamp;
    }

    public void setBlockStartTimestamp(String blockStartTimestamp) {
        this.blockStartTimestamp = blockStartTimestamp;
    }

    public String getJobStartTime() {
        return jobStartTime;
    }

    public void setJobStartTime(String jobStartTime) {
        this.jobStartTime = jobStartTime;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public JobRequestBody() {
    }

    public JobRequestBody(String sourcePattern, String targetPattern, String blockStartTimestamp, String jobStartTime, String interval) {
        this.sourcePattern = sourcePattern;
        this.targetPattern = targetPattern;
        this.blockStartTimestamp = blockStartTimestamp;
        this.jobStartTime = jobStartTime;
        this.interval = interval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobRequestBody that = (JobRequestBody) o;

        if (sourcePattern != null ? !sourcePattern.equals(that.sourcePattern) : that.sourcePattern != null)
            return false;
        if (targetPattern != null ? !targetPattern.equals(that.targetPattern) : that.targetPattern != null)
            return false;
        if (blockStartTimestamp != null ? !blockStartTimestamp.equals(that.blockStartTimestamp) : that.blockStartTimestamp != null)
            return false;
        if (jobStartTime != null ? !jobStartTime.equals(that.jobStartTime) : that.jobStartTime != null) return false;
        return interval != null ? interval.equals(that.interval) : that.interval == null;
    }

    @Override
    public int hashCode() {
        int result = sourcePattern != null ? sourcePattern.hashCode() : 0;
        result = 31 * result + (targetPattern != null ? targetPattern.hashCode() : 0);
        result = 31 * result + (blockStartTimestamp != null ? blockStartTimestamp.hashCode() : 0);
        result = 31 * result + (jobStartTime != null ? jobStartTime.hashCode() : 0);
        result = 31 * result + (interval != null ? interval.hashCode() : 0);
        return result;
    }
}
