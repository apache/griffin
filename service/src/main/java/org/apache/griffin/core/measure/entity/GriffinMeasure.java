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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.collections.CollectionUtils;
import org.apache.griffin.core.util.JsonUtil;
import org.springframework.util.StringUtils;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Measures processed on Griffin
 */
@Entity
public class GriffinMeasure extends Measure {
    public enum ProcessType {
        /**
         * Currently we just support BATCH and STREAMING type
         */
        BATCH,
        STREAMING
    }

    @Enumerated(EnumType.STRING)
    private ProcessType processType;
	private static final long serialVersionUID = -475176898459647661L;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long timestamp;

    @JsonIgnore
//    @Access(AccessType.PROPERTY)
    @Column(length = 1024)
    private String ruleDescription;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> ruleDescriptionMap;

    @NotNull
    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "measure_id")
    private List<DataSource> dataSources = new ArrayList<>();

    @NotNull
    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "evaluate_rule_id")
    private EvaluateRule evaluateRule;

    @JsonProperty("process.type")
    public ProcessType getProcessType() {
        return processType;
    }

    @JsonProperty("process.type")
    public void setProcessType(ProcessType processType) {
        this.processType = processType;
    }

    @JsonProperty("data.sources")
    public List<DataSource> getDataSources() {
        return dataSources;
    }

    @JsonProperty("data.sources")
    public void setDataSources(List<DataSource> dataSources) {
        if (CollectionUtils.isEmpty(dataSources)) {
            throw new NullPointerException("Data source can not be empty.");
        }
        this.dataSources = dataSources;
    }

    @JsonProperty("evaluate.rule")
    public EvaluateRule getEvaluateRule() {
        return evaluateRule;
    }

    @JsonProperty("evaluate.rule")
    public void setEvaluateRule(EvaluateRule evaluateRule) {
        if (evaluateRule == null || CollectionUtils.isEmpty(evaluateRule.getRules())) {
            throw new NullPointerException("Evaluate rule can not be empty.");
        }
        this.evaluateRule = evaluateRule;
    }

    @JsonProperty("rule.description")
    public Map<String, Object> getRuleDescriptionMap() {
        return ruleDescriptionMap;
    }

    @JsonProperty("rule.description")
    public void setRuleDescriptionMap(Map<String, Object> ruleDescriptionMap) {
        this.ruleDescriptionMap = ruleDescriptionMap;
    }


    private String getRuleDescription() {
        return ruleDescription;
    }

    private void setRuleDescription(String ruleDescription) {
        this.ruleDescription = ruleDescription;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String getType() {
        return "griffin";
    }

    public GriffinMeasure() {
        super();
    }

    public GriffinMeasure(String name, String owner, List<DataSource> dataSources, EvaluateRule evaluateRule) {
        this.name = name;
        this.owner = owner;
        this.dataSources = dataSources;
        this.evaluateRule = evaluateRule;
    }

    public GriffinMeasure(Long measureId, String name, String owner, List<DataSource> dataSources, EvaluateRule evaluateRule) {
        this.setId(measureId);
        this.name = name;
        this.owner = owner;
        this.dataSources = dataSources;
        this.evaluateRule = evaluateRule;
    }

    @PrePersist
    @PreUpdate
    public void save() throws JsonProcessingException {
        if (ruleDescriptionMap != null) {
            this.ruleDescription = JsonUtil.toJson(ruleDescriptionMap);
        }
    }

    @PostLoad
    public void load() throws IOException {
        if (!StringUtils.isEmpty(ruleDescription)) {
            this.ruleDescriptionMap = JsonUtil.toEntity(ruleDescription, new TypeReference<Map<String, Object>>() {
            });
        }
    }
}
