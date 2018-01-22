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

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@DiscriminatorValue("griffin_job")
public class GriffinJob extends AbstractJob {

    @Column(name = "quartz_job_name")
    private String quartzName;

    @Column(name = "quartz_group_name")
    private String quartzGroup;

    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE}, orphanRemoval = true)
    @JoinColumn(name = "job_id")
    private List<JobInstanceBean> jobInstances = new ArrayList<>();

    public String getQuartzName() {
        return quartzName;
    }

    public void setQuartzName(String quartzName) {
        this.quartzName = quartzName;
    }

    public String getQuartzGroup() {
        return quartzGroup;
    }

    public void setQuartzGroup(String quartzGroup) {
        this.quartzGroup = quartzGroup;
    }

    public List<JobInstanceBean> getJobInstances() {
        return jobInstances;
    }

    public void setJobInstances(List<JobInstanceBean> jobInstances) {
        this.jobInstances = jobInstances;
    }

    public GriffinJob() {
        super();
    }

    public GriffinJob(Long measureId, String jobName, String quartzName, String quartzGroup, boolean deleted) {
        super(measureId, jobName, deleted);
        this.metricName = jobName;
        this.quartzName = quartzName;
        this.quartzGroup = quartzGroup;
    }

    public GriffinJob(Long jobId, Long measureId, String jobName, String qJobName, String qGroupName, boolean deleted) {
        this(measureId, jobName, qJobName, qGroupName, deleted);
        setId(jobId);
    }
}
