/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

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
