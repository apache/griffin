package org.apache.griffin.core.config;

import java.util.Map;

import javax.sql.DataSource;

import org.apache.griffin.core.util.PropertiesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import com.google.common.collect.Maps;

@Configuration
public class DatasourceConfig {

    @Autowired
    public DatasourceConfig(@Value("${external.config.location}") String configLocation,
            ConfigurableEnvironment environment) {
        MapPropertySource propertySource = new MapPropertySource("datasources",
                PropertiesUtil.getYamlProperties("datasources.yml", "", configLocation));
        environment.getPropertySources().addFirst(propertySource);
    }

    @Bean(name = "jdbcDatasources")
    @ConfigurationProperties(prefix = "ds")
    public Map<String, Map<String, DataSource>> getJdbcDatasources() {
        return Maps.newLinkedHashMap();
    }
}
