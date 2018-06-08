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
import org.apache.griffin.core.util.JsonUtil;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.persistence.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Entity
public class DataSource extends AbstractAuditableEntity {
    private static final long serialVersionUID = -4748881017079815794L;

    private String name;

    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "data_source_id")
    private List<DataConnector> connectors = new ArrayList<>();

    @JsonIgnore
    @Column(length = 1024)
    private String cache;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> cacheMap;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<DataConnector> getConnectors() {
        return connectors;
    }

    public void setConnectors(List<DataConnector> connectors) {
        if (CollectionUtils.isEmpty(connectors)) {
            throw new NullPointerException("Data connector can not be empty.");
        }
        this.connectors = connectors;
    }

    private String getCache() {
        return cache;
    }

    private void setCache(String cache) {
        this.cache = cache;

    }

    @JsonProperty("cache")
    public Map<String, Object> getCacheMap() {
        return cacheMap;
    }

    @JsonProperty("cache")
    public void setCacheMap(Map<String, Object> cacheMap) {
        this.cacheMap = cacheMap;
    }

    @PrePersist
    @PreUpdate
    public void save() throws JsonProcessingException {
        if (cacheMap != null) {
            this.cache = JsonUtil.toJson(cacheMap);
        }
    }

    @PostLoad
    public void load() throws IOException {
        if (!StringUtils.isEmpty(cache)) {
            this.cacheMap = JsonUtil.toEntity(cache, new TypeReference<Map<String, Object>>() {
            });
        }
    }

    public DataSource() {
    }

    public DataSource(String name, List<DataConnector> connectors) {
        this.name = name;
        this.connectors = connectors;
    }
}
