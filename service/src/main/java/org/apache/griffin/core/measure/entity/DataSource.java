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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.PostLoad;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Transient;

import org.apache.griffin.core.util.JsonUtil;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

@Entity
public class DataSource extends AbstractAuditableEntity {
    private static final long serialVersionUID = -4748881017079815794L;

    private String name;

    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST,
            CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "data_source_id")
    private List<DataConnector> connectors = new ArrayList<>();

    private boolean baseline = false;

    @JsonIgnore
    @Column(length = 1024)
    private String checkpoint;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> checkpointMap;


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

    public boolean isBaseline() {
        return baseline;
    }

    public void setBaseline(boolean baseline) {
        this.baseline = baseline;
    }

    private String getCheckpoint() {
        return checkpoint;
    }

    private void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;

    }

    @JsonProperty("checkpoint")
    public Map<String, Object> getCheckpointMap() {
        return checkpointMap;
    }

    public void setCheckpointMap(Map<String, Object> checkpointMap) {
        this.checkpointMap = checkpointMap;
    }

    @PrePersist
    @PreUpdate
    public void save() throws JsonProcessingException {
        if (checkpointMap != null) {
            this.checkpoint = JsonUtil.toJson(checkpointMap);
        }
    }

    @PostLoad
    public void load() throws IOException {
        if (!StringUtils.isEmpty(checkpoint)) {
            this.checkpointMap = JsonUtil.toEntity(
                checkpoint, new TypeReference<Map<String, Object>>() {
                });
        }
    }

    public DataSource() {
    }

    public DataSource(String name, List<DataConnector> connectors) {
        this.name = name;
        this.connectors = connectors;
    }

    public DataSource(String name, boolean baseline,
                      Map<String, Object> checkpointMap,
                      List<DataConnector> connectors) {
        this.name = name;
        this.baseline = baseline;
        this.checkpointMap = checkpointMap;
        this.connectors = connectors;

    }
}
