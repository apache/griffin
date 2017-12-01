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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.griffin.core.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Entity;
import javax.persistence.Transient;
import java.io.IOException;
import java.util.Map;

@Entity
public class DataConnector extends AbstractAuditableEntity {
    private static final long serialVersionUID = -4748881017029815594L;

    private final static Logger LOGGER = LoggerFactory.getLogger(DataConnector.class);

    private String type;

    private String version;

    @JsonIgnore
    @Access(AccessType.PROPERTY)
    private String config;

    @Transient
    private Map<String, String> configMap;

    @JsonProperty("config")
    public Map<String, String> getConfigMap() throws IOException {
        return configMap;
    }

    @JsonProperty("config")
    public void setConfigMap(Map<String, String> configMap) throws JsonProcessingException {
        this.configMap = configMap;
        this.config = JsonUtil.toJson(configMap);
    }

    public void setConfig(String config) throws IOException {
        this.config = config;
        this.configMap = JsonUtil.toEntity(config, new TypeReference<Map<String, String>>() {
        });
    }

    public String getConfig() throws IOException {
        return config;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }


    public DataConnector() {
    }

    public DataConnector(String type, String version, String config) throws IOException {
        this.type = type;
        this.version = version;
        this.config = config;
        this.configMap = JsonUtil.toEntity(config, new TypeReference<Map<String, String>>() {
        });
    }

    @Override
    public String toString() {
        return "DataConnector{" +
                "type=" + type +
                ", version='" + version + '\'' +
                ", config=" + config +
                '}';
    }
}
