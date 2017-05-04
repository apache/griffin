package org.apache.griffin.core.measure;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import java.util.HashMap;
import java.util.Map;

@Entity
public class DataConnector extends AuditableEntity  {

    private static final long serialVersionUID = -4748881017029815794L;
    
    public enum ConnectorType {
        HIVE
    }
    
    @Enumerated(EnumType.STRING)
    public ConnectorType type;
    
    public String version;

    private String config;

    public Map<String,String> getConfig() {
        Map<String, String> map = new HashMap<String, String>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            //convert JSON string to Map
            map = mapper.readValue(config, new TypeReference<HashMap<String, String>>() {});
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public void setConfig(Map<String,String> config) throws JsonProcessingException {
        String configJson = new ObjectMapper().writeValueAsString(config);
        this.config = configJson;
    }

    public DataConnector() {
    }

    public DataConnector(ConnectorType type, String version, String config) {
        super();
        this.type = type;
        this.version = version;
        this.config = config;
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
