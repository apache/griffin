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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;

import javax.persistence.*;

@Entity
@Table(name = "job")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "type")
public abstract class AbstractJob extends AbstractAuditableEntity {
    private static final long serialVersionUID = 7569493377868453677L;

    protected Long measureId;

    protected String jobName;

    protected String metricName;

    @Column(name = "quartz_job_name")
    private String name;

    @Column(name = "quartz_group_name")
    private String group;

    @JsonIgnore
    protected boolean deleted = false;

    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn(name = "job_schedule_id")
    private JobSchedule jobSchedule;

    AbstractJob() {
    }

    AbstractJob(Long measureId, String jobName, String name, String group, boolean deleted) {
        this.measureId = measureId;
        this.jobName = jobName;
        this.name = name;
        this.group = group;
        this.deleted = deleted;
    }

    AbstractJob(String jobName, Long measureId, String metricName) {
        this.jobName = jobName;
        this.measureId = measureId;
        this.metricName = metricName;
    }

    @JsonProperty("job.name")
    public String getJobName() {
        return jobName;
    }

    @JsonProperty("job.name")
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @JsonProperty("metric.name")
    public String getMetricName() {
        return metricName;
    }

    @JsonProperty("metric.name")
    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    @JsonProperty("measure.id")
    public Long getMeasureId() {
        return measureId;
    }

    @JsonProperty("measure.id")
    public void setMeasureId(Long measureId) {
        this.measureId = measureId;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @JsonProperty("job.config")
    public JobSchedule getJobSchedule() {
        return jobSchedule;
    }

    @JsonProperty("job.config")
    public void setJobSchedule(JobSchedule jobSchedule) {
        this.jobSchedule = jobSchedule;
    }

    @JsonProperty("quartz.name")
    public String getName() {
        return name;
    }

    @JsonProperty("quartz.name")
    public void setName(String quartzName) {
        this.name = quartzName;
    }

    @JsonProperty("quartz.group")
    public String getGroup() {
        return group;
    }

    @JsonProperty("quartz.group")
    public void setGroup(String quartzGroup) {
        this.group = quartzGroup;
    }
}
