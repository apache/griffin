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
import java.util.Map;

@Entity
public class SegmentPredicate extends AbstractAuditableEntity {

    private String type;

    @JsonIgnore
//    @Access(AccessType.PROPERTY)
    private String config;

    @Transient
    private Map<String, String> configMap;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("config")
    public Map<String, String> getConfigMap() {
        return configMap;
    }

    @JsonProperty("config")
    public void setConfigMap(Map<String, String> configMap) {
        this.configMap = configMap;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    @PrePersist
    @PreUpdate
    public void save() throws JsonProcessingException {
        if (configMap != null) {
            this.config = JsonUtil.toJson(configMap);
        }
    }

    @PostLoad
    public void load() throws IOException {
        if (!StringUtils.isEmpty(config)) {
            this.configMap = JsonUtil.toEntity(config, new TypeReference<Map<String, Object>>() {
            });
        }
    }

    public SegmentPredicate() {
    }

    public SegmentPredicate(String type, Map configMap) throws JsonProcessingException {
        this.type = type;
        this.config = JsonUtil.toJson(configMap);
    }
}
