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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.collections.CollectionUtils;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Measures processed on Griffin
 */
@Entity
public class GriffinMeasure extends Measure {

    private String processType;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long timestamp;


    @NotNull
    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "measure_id")
    private List<DataSource> dataSources = new ArrayList<>();

    @NotNull
    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "evaluate_rule_id")
    private EvaluateRule evaluateRule;

    @JsonProperty("process.type")
    public String getProcessType() {
        return processType;
    }

    @JsonProperty("process.type")
    public void setProcessType(String processType) {
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

    public GriffinMeasure(Long measureId,String name, String description, String organization, String processType, String owner, List<DataSource> dataSources, EvaluateRule evaluateRule) {
        super(name, description, organization, owner);
        this.setId(measureId);
        this.processType = processType;
        this.dataSources = dataSources;
        this.evaluateRule = evaluateRule;
    }

}
