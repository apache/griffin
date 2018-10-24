package org.apache.griffin.core.config;

import org.apache.griffin.core.util.PropertiesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

@Configuration
public class DatasourceConfig {

    @Autowired
    public DatasourceConfig(@Value("${external.config.location}") String configLocation,
            ConfigurableEnvironment environment) {
        MapPropertySource propertySource = new MapPropertySource("datasources",
                PropertiesUtil.getYamlProperties("datasources.yml", "", configLocation));
        environment.getPropertySources().addFirst(propertySource);
    }
}
