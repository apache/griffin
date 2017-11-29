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
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;
import org.apache.griffin.core.util.JsonUtil;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Entity
public class JobSchedule extends AbstractAuditableEntity {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobSchedule.class);

    private Long measureId;

    private String cronExpression;

    private String timeZone;

    private String predicateConfig;

    @JsonIgnore
    @Transient
    private Map<String, String> configMap;

    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "job_schedule_id")
    private List<JobDataSegment> segments = new ArrayList<>();

    @JsonProperty("measure.id")
    public Long getMeasureId() {
        return measureId;
    }

    @JsonProperty("measure.id")
    public void setMeasureId(Long measureId) {
        this.measureId = measureId;
    }

    @JsonProperty("cron.expression")
    public String getCronExpression() {
        return cronExpression;
    }

    @JsonProperty("cron.expression")
    public void setCronExpression(String cronExpression) {
        if (StringUtils.isEmpty(cronExpression) ||  !isCronExpressionValid(cronExpression)) {
            throw new IllegalArgumentException("Cron expression is invalid.Please check your cron expression.");
        }
        this.cronExpression = cronExpression;
    }

    @JsonProperty("cron.time.zone")
    public String getTimeZone() {
        return timeZone;
    }

    @JsonProperty("cron.time.zone")
    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    @JsonProperty("data.segments")
    public List<JobDataSegment> getSegments() {
        return segments;
    }

    @JsonProperty("data.segments")
    public void setSegments(List<JobDataSegment> segments) {
        this.segments = segments;
    }

    @JsonProperty("predicate.config")
    public String getPredicateConfig() {
        return predicateConfig;
    }

    @JsonProperty("predicate.config")
    public void setPredicateConfig(Map<String, String> configMap) throws JsonProcessingException {
        this.setConfigMap(configMap);
        this.predicateConfig = JsonUtil.toJson(configMap);
    }

    public Map<String, String> getConfigMap() throws IOException {
        if (configMap == null) {
            configMap = JsonUtil.toEntity(predicateConfig, Map.class);
        }
        return configMap;
    }

    private void setConfigMap(Map<String, String> configMap) {
        this.configMap = configMap;
    }

    private boolean isCronExpressionValid(String cronExpression) {
        if (!CronExpression.isValidExpression(cronExpression)) {
            LOGGER.error("Cron expression {} is invalid.", cronExpression);
            return false;
        }
        return true;
    }

    public JobSchedule() {
    }

    public JobSchedule(Long measureId, String cronExpression, Map predicateConfig, List<JobDataSegment> segments) throws JsonProcessingException {
        this.measureId = measureId;
        this.cronExpression = cronExpression;
        setPredicateConfig(predicateConfig);
        this.segments = segments;
    }
}
