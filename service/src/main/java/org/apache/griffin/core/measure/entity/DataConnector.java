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
import org.apache.griffin.core.job.entity.SegmentPredicate;
import org.apache.griffin.core.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Entity
public class DataConnector extends AbstractAuditableEntity {
    private static final long serialVersionUID = -4748881017029815594L;

    private final static Logger LOGGER = LoggerFactory.getLogger(DataConnector.class);

    @NotNull
    private String name;

    private String type;

    private String version;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String dataUnit;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String dataTimeZone;

    @JsonIgnore
    @Transient
    private String defaultDataUnit = "365000d";

    @JsonIgnore
//    @Access(AccessType.PROPERTY)
    private String config;

    @Transient
    private Map<String, String> configMap;

    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "data_connector_id")
    private List<SegmentPredicate> predicates = new ArrayList<>();

    public List<SegmentPredicate> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<SegmentPredicate> predicates) {
        this.predicates = predicates;
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

    public void setConfig(String config) throws IOException {
        if (!StringUtils.isEmpty(config)) {
            this.config = config;
            this.configMap = JsonUtil.toEntity(config, new TypeReference<Map<String, String>>() {
            });
        }
    }

    public String getConfig() {
        return config;
    }

    @JsonProperty("data.unit")
    public String getDataUnit() {
        return dataUnit;
    }

    @JsonProperty("data.unit")
    public void setDataUnit(String dataUnit) {
        this.dataUnit = dataUnit;
    }

    @JsonProperty("data.time.zone")
    public String getDataTimeZone() {
        return dataTimeZone;
    }

    @JsonProperty("data.time.zone")
    public void setDataTimeZone(String dataTimeZone) {
        this.dataTimeZone = dataTimeZone;
    }

    public String getDefaultDataUnit() {
        return defaultDataUnit;
    }

    public void setDefaultDataUnit(String defaultDataUnit) {
        this.defaultDataUnit = defaultDataUnit;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (StringUtils.isEmpty(name)) {
            LOGGER.warn("Connector name cannot be empty.");
            throw new NullPointerException();
        }
        this.name = name;
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

    public DataConnector(String name, String type, String version, String config) throws IOException {
        this.name = name;
        this.type = type;
        this.version = version;
        this.config = config;
        this.configMap = JsonUtil.toEntity(config, new TypeReference<Map<String, String>>() {
        });
    }

    public DataConnector(String name, String dataUnit, Map configMap, List<SegmentPredicate> predicates) throws IOException {
        this.name = name;
        this.dataUnit = dataUnit;
        this.configMap = configMap;
        this.predicates = predicates;
    }

    @Override
    public String toString() {
        return "DataConnector{" +
                "name=" + name +
                "type=" + type +
                ", version='" + version + '\'' +
                ", config=" + config +
                '}';
    }
}
