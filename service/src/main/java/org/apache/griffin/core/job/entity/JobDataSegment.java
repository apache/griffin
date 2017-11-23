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

import javax.persistence.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Entity
public class JobDataSegment extends AbstractAuditableEntity {

    private Long dataConnectorId;

    private String config;

    private String dataConnectorIndex;

    @JsonIgnore
    @Transient
    private Map<String, String> configMap;

    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "segment_id")
    private List<SegmentPredict> predicts;

    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name ="segment_split_id")
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

    public void setConfig(Map<String, String> configMap) throws JsonProcessingException {
        this.configMap = configMap;
        this.config = JsonUtil.toJson(configMap);
    }

    public Map<String, String> getConfigMap() throws IOException {
        if(configMap == null && !StringUtils.isEmpty(config)){
            configMap = JsonUtil.toEntity(config, new TypeReference<Map<String,String>>(){});
        }
        return configMap;
    }

    public void setConfigMap(Map<String, String> configMap) {
        this.configMap = configMap;
    }


    public List<SegmentPredict> getPredicts() {
        return predicts;
    }

    public void setPredicts(List<SegmentPredict> predicts) {
        this.predicts = predicts;
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
