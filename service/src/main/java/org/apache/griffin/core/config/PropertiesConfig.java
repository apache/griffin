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

import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

@Configuration
public class PropertiesConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesConfig.class);

    private String location;

    public PropertiesConfig(@Value("${external.config.location}") String location) {
        LOGGER.info("external.config.location : {}", location);
        this.location = location;
    }

    private String getPath(String defaultPath, String name) throws FileNotFoundException {
        String path = defaultPath;
        File file = new File(location);
        LOGGER.info("File absolute path:" + file.getAbsolutePath());
        File[] files = file.listFiles();
        if (files == null || files.length == 0) {
            LOGGER.error("The defaultPath {} does not exist.Please check your config in application.properties.", location);
            throw new FileNotFoundException();
        }
        for (File f : files) {
            if (f.getName().equals(name)) {
                path = location + File.separator + name;
                LOGGER.info("config real path: {}", path);
            }
        }
        return path;
    }


    @Bean(name = "appConf")
    public Properties appConf() {
        String path = "/application.properties";
        return PropertiesUtil.getProperties(path, new ClassPathResource(path));
    }

    @Bean(name = "livyConf")
    public Properties livyConf() throws FileNotFoundException {
        String path = "/sparkJob.properties";
        if (StringUtils.isEmpty(location)) {
            return PropertiesUtil.getProperties(path, new ClassPathResource(path));
        }
        path = getPath(path, "sparkJob.properties");
        Resource resource = new InputStreamResource(new FileInputStream(path));
        return PropertiesUtil.getProperties(path, resource);
    }

    @Bean(name = "quartzConf")
    public Properties quartzConf() throws FileNotFoundException {
        String path = "/quartz.properties";
        if (StringUtils.isEmpty(location)) {
            return PropertiesUtil.getProperties(path, new ClassPathResource(path));
        }
        path = getPath(path, "quartz.properties");
        Resource resource = new InputStreamResource(new FileInputStream(path));
        return PropertiesUtil.getProperties(path, resource);
    }
}
