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

    protected Boolean deleted = false;

    AbstractJob() {
    }

    AbstractJob(Long measureId, String jobName, boolean deleted) {
        this.measureId = measureId;
        this.jobName = jobName;
        this.deleted = deleted;
    }

    AbstractJob(String jobName, Long measureId, String metricName) {
        this.jobName = jobName;
        this.measureId = measureId;
        this.metricName = metricName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public Long getMeasureId() {
        return measureId;
    }

    public void setMeasureId(Long measureId) {
        this.measureId = measureId;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

}
