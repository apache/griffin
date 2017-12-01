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
import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;
import org.apache.griffin.core.util.JsonUtil;

import javax.persistence.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Entity
public class JobDataSegment extends AbstractAuditableEntity {

    private Long dataConnectorId;

    private String dataConnectorIndex;

    @JsonIgnore
    @Access(AccessType.PROPERTY)
    private String config;

    @Transient
    private Map<String, String> configMap;

    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "segment_id")
    private List<SegmentPredicate> predicates = new ArrayList<>();

    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "segment_split_id")
    private SegmentSplit segmentSplit;

    @JsonProperty("data.connector.id")
    public Long getDataConnectorId() {
        return dataConnectorId;
    }

    @JsonProperty("data.connector.id")
    public void setDataConnectorId(Long dataConnectorId) {
        this.dataConnectorId = dataConnectorId;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) throws IOException {
        this.config = config;
        this.configMap = JsonUtil.toEntity(config, new TypeReference<Map<String, String>>() {
        });
    }

    @JsonProperty("config")
    public Map<String, String> getConfigMap() {
        return configMap;
    }

    @JsonProperty("config")
    public void setConfigMap(Map<String, String> configMap) throws JsonProcessingException {
        this.configMap = configMap;
        this.config = JsonUtil.toJson(configMap);
    }


    public List<SegmentPredicate> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<SegmentPredicate> predicates) {
        if (predicates == null) {
            predicates = new ArrayList<>();
        }
        this.predicates = predicates;
    }

    @JsonProperty("segment.split")
    public SegmentSplit getSegmentSplit() {
        return segmentSplit;
    }

    @JsonProperty("segment.split")
    public void setSegmentSplit(SegmentSplit segmentSplit) {
        this.segmentSplit = segmentSplit;
    }

    @JsonProperty("data.connector.index")
    public String getDataConnectorIndex() {
        return dataConnectorIndex;
    }

    @JsonProperty("data.connector.index")
    public void setDataConnectorIndex(String dataConnectorIndex) {
        this.dataConnectorIndex = dataConnectorIndex;
    }

    public JobDataSegment() {
    }
}
