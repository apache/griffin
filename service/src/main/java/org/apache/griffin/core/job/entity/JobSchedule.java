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
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.griffin.core.util.PropertiesUtil;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.*;

@Entity
public class JobSchedule extends AbstractAuditableEntity {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobSchedule.class);

    @NotNull
    private Long measureId;

    @NotNull
    private String jobName;

    @NotNull
    private String cronExpression;

    @NotNull
    private String timeZone;

    @JsonIgnore
    @Access(AccessType.PROPERTY)
    private String predicateConfig;

    @Transient
    private Map<String, Object> configMap = defaultPredicatesConfig();

    @NotNull
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

    @JsonProperty("job.name")
    public String getJobName() {
        return jobName;
    }

    @JsonProperty("job.name")
    public void setJobName(String jobName) {
        if (StringUtils.isEmpty(jobName)) {
            LOGGER.error("Job name cannot be empty.");
            throw new NullPointerException();
        }
        this.jobName = jobName;
    }

    @JsonProperty("cron.expression")
    public String getCronExpression() {
        return cronExpression;
    }

    @JsonProperty("cron.expression")
    public void setCronExpression(String cronExpression) {
        if (StringUtils.isEmpty(cronExpression) || !isCronExpressionValid(cronExpression)) {
            LOGGER.error("Cron expression is invalid.Please check your cron expression.");
            throw new IllegalArgumentException();
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

    private String getPredicateConfig() {
        return predicateConfig;
    }

    private void setPredicateConfig(String config) throws IOException {
        this.predicateConfig = config;
        this.configMap = JsonUtil.toEntity(config, new TypeReference<Map<String, Object>>() {
        });
    }

    @JsonProperty("predicate.config")
    public Map<String, Object> getConfigMap() throws IOException {
        return configMap;
    }

    @JsonProperty("predicate.config")
    private void setConfigMap(Map<String, Object> configMap) throws JsonProcessingException {
        this.configMap = configMap;
        this.predicateConfig = JsonUtil.toJson(configMap);
    }

    /**
     * @return set default predicate config
     * @throws JsonProcessingException json exception
     */
    private Map<String, Object> defaultPredicatesConfig() throws JsonProcessingException {
        String path = "/application.properties";
        Properties appConf = PropertiesUtil.getProperties(path, new ClassPathResource(path));
        Map<String, Object> scheduleConf = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        map.put("interval", appConf.getProperty("predicate.job.interval"));
        map.put("repeat", appConf.getProperty("predicate.job.repeat.count"));
        scheduleConf.put("checkdonefile.schedule", map);
        setConfigMap(scheduleConf);
        return scheduleConf;
    }

    private boolean isCronExpressionValid(String cronExpression) {
        if (!CronExpression.isValidExpression(cronExpression)) {
            LOGGER.error("Cron expression {} is invalid.", cronExpression);
            return false;
        }
        return true;
    }

    public JobSchedule() throws JsonProcessingException {
    }

    public JobSchedule(Long measureId, String jobName, String cronExpression, String timeZone, List<JobDataSegment> segments) throws JsonProcessingException {
        this.measureId = measureId;
        this.jobName = jobName;
        this.cronExpression = cronExpression;
        this.timeZone = timeZone;
        this.segments = segments;
    }
}
