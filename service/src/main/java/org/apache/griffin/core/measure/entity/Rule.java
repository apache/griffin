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

package org.apache.griffin.core.measure.entity;

import java.io.IOException;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.PostLoad;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Transient;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;


@Entity
public class Rule extends AbstractAuditableEntity {
	private static final long serialVersionUID = -143019093509759648L;

	/**
     * three type:1.griffin-dsl 2.df-opr 3.spark-sql
     */
    @NotNull
    private String dslType;

    @NotNull
    private String dqType;

    @Column(length = 8 * 1024)
    @NotNull
    private String rule;

    private String name;

    @JsonIgnore
//    @Access(AccessType.PROPERTY)
    @Column(length = 1024)
    private String details;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> detailsMap;

    @JsonIgnore
//    @Access(AccessType.PROPERTY)
    private String metric;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> metricMap;

    @JsonIgnore
//    @Access(AccessType.PROPERTY)
    private String record;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> recordMap;

    @JsonProperty("dsl.type")
    public String getDslType() {
        return dslType;
    }

    @JsonProperty("dsl.type")
    public void setDslType(String dslType) {
        this.dslType = dslType;
    }

    @JsonProperty("dq.type")
    public String getDqType() {
        return dqType;
    }

    @JsonProperty("dq.type")
    public void setDqType(String dqType) {
        this.dqType = dqType;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    @JsonProperty("details")
    public Map<String, Object> getDetailsMap() {
        return detailsMap;
    }

    @JsonProperty("details")
    public void setDetailsMap(Map<String, Object> detailsMap) {
        this.detailsMap = detailsMap;
    }

    @JsonProperty("metric")
    public Map<String, Object> getMetricMap() {
        return metricMap;
    }

    @JsonProperty("metric")
    public void setMetricMap(Map<String, Object> metricMap) {
        this.metricMap = metricMap;
    }

    @JsonProperty("record")
    public Map<String, Object> getRecordMap() {
        return recordMap;
    }

    @JsonProperty("record")
    public void setRecordMap(Map<String, Object> recordMap) {
        this.recordMap = recordMap;
    }

    private String getDetails() {
        return details;
    }

    private void setDetails(String details) {
        this.details = details;
    }

    private void setMetric(String metric) {
        this.metric = metric;
    }

    private String getRecord() {
        return record;
    }

    private void setRecord(String record) {
        this.record = record;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @PrePersist
    @PreUpdate
    public void save() throws JsonProcessingException {
        if (detailsMap != null) {
            this.details = JsonUtil.toJson(detailsMap);
        }
        if (metricMap != null) {
            this.metric = JsonUtil.toJson(metricMap);
        }
        if (recordMap != null) {
            this.record = JsonUtil.toJson(recordMap);
        }

    }

    @PostLoad
    public void load() throws IOException {
        if (!StringUtils.isEmpty(details)) {
            this.detailsMap = JsonUtil.toEntity(details, new TypeReference<Map<String, Object>>() {
            });
        }
        if (!StringUtils.isEmpty(metric)) {
            this.metricMap = JsonUtil.toEntity(metric, new TypeReference<Map<String, Object>>() {
            });
        }
        if (!StringUtils.isEmpty(record)) {
            this.recordMap = JsonUtil.toEntity(record, new TypeReference<Map<String, Object>>() {
            });
        }
    }

    public Rule() {
    }

    public Rule(String dslType, String dqType, String rule, Map<String, Object> detailsMap) throws JsonProcessingException {
        this.dslType = dslType;
        this.dqType = dqType;
        this.rule = rule;
        this.details = JsonUtil.toJson(detailsMap);
    }
}
