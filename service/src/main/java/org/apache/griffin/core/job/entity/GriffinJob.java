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

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("griffin_job")
public class GriffinJob extends AbstractJob {

    private String quartzJobName;

    private String quartzGroupName;

    private String predicateJobName;

    private String predicateGroupName;

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

    public String getPredicateJobName() {
        return predicateJobName;
    }

    public void setPredicateJobName(String predicateJobName) {
        this.predicateJobName = predicateJobName;
    }

    public String getPredicateGroupName() {
        return predicateGroupName;
    }

    public void setPredicateGroupName(String predicateGroupName) {
        this.predicateGroupName = predicateGroupName;
    }


    public GriffinJob() {
        super();
    }

    public GriffinJob(Long measureId, String jobName, String qJobName, String qGroupName, boolean deleted) {
        super(measureId, jobName, deleted);
        this.quartzJobName = qJobName;
        this.quartzGroupName = qGroupName;
    }

    public GriffinJob(Long jobId, Long measureId, String jobName, String qJobName, String qGroupName, boolean deleted) {
        super(jobId, measureId, jobName, deleted);
        this.quartzJobName = qJobName;
        this.quartzGroupName = qGroupName;
    }

}
