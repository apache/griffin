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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.griffin.core.util.GriffinUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Transient;
import java.io.IOException;
import java.util.Map;

@Entity
public class DataConnector extends AuditableEntity  {
    private final static Logger log = LoggerFactory.getLogger(DataConnector.class);

    private static final long serialVersionUID = -4748881017029815794L;
    
    public enum ConnectorType {
        HIVE
    }
    
    @Enumerated(EnumType.STRING)
    private ConnectorType type;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    private String version;

    private String config;

    @JsonIgnore
    @Transient
    private Map<String,String> configInMaps;

    public Map<String,String> getConfigInMaps() {
        TypeReference<Map<String,String>> mapType=new TypeReference<Map<String,String>>(){};
        if (this.configInMaps == null) {
            try {
                this.configInMaps = GriffinUtil.toEntity(config, mapType);
            } catch (IOException e) {
                log.error("Error in converting json to map",e);
            }
        }
        return configInMaps;
    }

    public void setConfig(Map<String,String> configInMaps) throws JsonProcessingException {
        String configJson = GriffinUtil.toJson(configInMaps);
        this.config = configJson;
    }

    public ConnectorType getType() {
        return type;
    }

    public void setType(ConnectorType type) {
        this.type = type;
    }

    public Map<String,String> getConfig() {
        return getConfigInMaps();
    }


    public DataConnector() {
    }

    public DataConnector(ConnectorType type,String version, Map<String,String> config){
        this.type = type;
        this.version = version;
        this.configInMaps = config;
        this.config = GriffinUtil.toJson(configInMaps);
    }

    public DataConnector(ConnectorType type, String version, String config) {
        this.type = type;
        this.version = version;
        this.config = config;
        TypeReference<Map<String,String>> mapType=new TypeReference<Map<String,String>>(){};
        try {
            this.configInMaps = GriffinUtil.toEntity(config,mapType);
        } catch (IOException e) {
            log.error("Error in converting json to map",e);
        }
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
