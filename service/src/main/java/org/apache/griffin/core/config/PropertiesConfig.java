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

package org.apache.griffin.core.config;

import org.apache.griffin.core.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import static org.apache.griffin.core.util.PropertiesUtil.getConf;
import static org.apache.griffin.core.util.PropertiesUtil.getProperties;

@Configuration
public class PropertiesConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesConfig.class);

    private String location;

    public PropertiesConfig(@Value("${external.config.location}") String location) {
        LOGGER.info("external.config.location : {}", location != null ? location : "null");
        this.location = location;
    }

//    @PostConstruct
//    public void init() throws IOException {
//        String batchName = "env_batch.json";
//        String batchPath = "env/" + batchName;
//        String streamingName = "env_streaming.json";
//        String streamingPath = "env/" + streamingName;
//        FileUtil.readBatchEnv(batchPath, batchName);
//        FileUtil.readStreamingEnv(streamingPath, streamingName);
//    }


    @Bean(name = "appConf")
    public Properties appConf() {
        String path = "/application.properties";
        return getProperties(path, new ClassPathResource(path));
    }

    @Bean(name = "livyConf")
    public Properties livyConf() throws FileNotFoundException {
        String name = "sparkJob.properties";
        String defaultPath = "/" + name;
        return getConf(name, defaultPath, location);
    }

    @Bean(name = "quartzConf")
    public Properties quartzConf() throws FileNotFoundException {
        String name = "quartz.properties";
        String defaultPath = "/" + name;
        return getConf(name, defaultPath, location);
    }
}
