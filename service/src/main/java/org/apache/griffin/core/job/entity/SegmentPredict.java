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
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;
import org.apache.griffin.core.util.JsonUtil;

import javax.persistence.Entity;
import javax.persistence.Transient;
import java.io.IOException;
import java.util.Map;

@Entity
public class SegmentPredict extends AbstractAuditableEntity {

    private String type;

    private String config;

    @JsonIgnore
    @Transient
    private Map<String, String> configMap;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> configMap) throws JsonProcessingException {
        this.setConfigMap(configMap);
        this.config = JsonUtil.toJson(configMap);
    }

    public Map<String, String> getConfigMap() throws IOException {
        if(configMap == null){
            configMap = JsonUtil.toEntity(config, Map.class);
        }
        return configMap;
    }

    public void setConfigMap(Map<String, String> configMap) {
        this.configMap = configMap;
    }

    public SegmentPredict() {
    }
}
