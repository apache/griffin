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
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.griffin.core.util.JsonUtil;

import javax.persistence.*;
import java.io.IOException;
import java.util.Map;


@Entity
public class Rule extends AbstractAuditableEntity {

    /**
     * three type:1.griffin-dsl 2.df-opr 3.spark-sql
     */
    private String dslType;

    private String dqType;

    @Column(length = 1024)
    private String rule;

    @JsonIgnore
    @Access(AccessType.PROPERTY)
    private String details;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> detailsMap;


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

    public String getDetails() {
        return details;
    }

    private void setDetails(String details) throws IOException {
        this.details = details;
        detailsMap = JsonUtil.toEntity(details, new TypeReference<Map<String, Object>>() {
        });
    }

    @JsonProperty("details")
    public Map<String, Object> getDetailsMap() {
        return detailsMap;
    }

    @JsonProperty("details")
    public void setDetailsMap(Map<String, Object> details) throws IOException {
        this.detailsMap = details;
        this.details = JsonUtil.toJson(details);
    }

    public Rule() {
    }

    public Rule(String dslType, String dqType, String rule, Map<String, Object> details) throws IOException {
        this.dslType = dslType;
        this.dqType = dqType;
        this.rule = rule;
        setDetailsMap(details);
    }
}
