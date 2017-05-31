package org.apache.griffin.core.measure;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Transient;
import java.util.Collections;
import java.util.HashMap;
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

    public static Map<String,String> convertJonsToMap(String jsonString){
        if(StringUtils.isEmpty(jsonString)) return Collections.EMPTY_MAP;
        else{
            Map<String, String> map = new HashMap<String, String>();
            ObjectMapper mapper = new ObjectMapper();

            try {
                map = mapper.readValue(jsonString, new TypeReference<HashMap<String, String>>() {});
            } catch (Exception e) {
                log.error("Error in converting json to map",e);
            }
            return map;

        }
    }

    public Map<String,String> getConfigInMaps() {
        if (this.configInMaps == null) this.configInMaps = convertJonsToMap(config);
        return configInMaps;
    }

    public void setConfig(Map<String,String> configInMaps) throws JsonProcessingException {
        String configJson = new ObjectMapper().writeValueAsString(configInMaps);
        this.config = configJson;
    }

    public DataConnector() {
        System.out.println();
    }

    public DataConnector(ConnectorType type,String version, Map<String,String> config){
        this.type = type;
        this.version = version;
        this.configInMaps = config;
        try {
            this.config = new ObjectMapper().writeValueAsString(configInMaps);
        } catch (JsonProcessingException e) {
            log.error("cannot convert map to josn in DataConnector",e);
            this.config = "";
        }
    }

    public DataConnector(ConnectorType type, String version, String config) {
        this.type = type;
        this.version = version;
        this.config = config;
        this.configInMaps = convertJonsToMap(config);
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
