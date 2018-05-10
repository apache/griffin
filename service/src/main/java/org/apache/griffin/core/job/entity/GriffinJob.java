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

@Entity
@DiscriminatorValue("griffin_job")
public class GriffinJob extends AbstractJob {

	private static final long serialVersionUID = 1019063003703509539L;

	@Column(name = "quartz_job_name")
    private String quartzName;

    @Column(name = "quartz_group_name")
    private String quartzGroup;


    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn(name = "job_schedule_id")
    private JobSchedule jobSchedule;

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

    public JobSchedule getJobSchedule() {
        return jobSchedule;
    }

    public void setJobSchedule(JobSchedule jobSchedule) {
        this.jobSchedule = jobSchedule;
    }


    public GriffinJob() {
        super();
    }

    public GriffinJob(Long measureId, String jobName, String quartzName, String quartzGroup, JobSchedule schedule,boolean deleted) {
        this(measureId, jobName, quartzName, quartzGroup, deleted);
        this.jobSchedule = schedule;
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
