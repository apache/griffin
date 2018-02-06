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

package org.apache.griffin.core.util;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);

    public static Properties getProperties(String path, Resource resource) {
        PropertiesFactoryBean propFactoryBean = new PropertiesFactoryBean();
        Properties properties = null;
        try {
            propFactoryBean.setLocation(resource);
            propFactoryBean.afterPropertiesSet();
            properties = propFactoryBean.getObject();
            LOGGER.info("Read properties successfully from {}.", path);
        } catch (IOException e) {
            LOGGER.error("Get properties from {} failed. {}", path, e);
        }
        return properties;
    }

    /**
     * @param name properties name like sparkJob.properties
     * @param defaultPath properties classpath like /application.properties
     * @param location custom properties path
     * @return Properties
     * @throws FileNotFoundException location setting is wrong that there is no target file.
     */
    public static Properties getConf(String name, String defaultPath, String location) throws FileNotFoundException {
        String path = getConfPath(name, location);
        Resource resource;
        if (path == null) {
            resource = new ClassPathResource(defaultPath);
            path = defaultPath;
        } else {
            resource = new InputStreamResource(new FileInputStream(path));
        }
        return PropertiesUtil.getProperties(path, resource);
    }

    private static String getConfPath(String name, String location) throws FileNotFoundException {
        if (StringUtils.isEmpty(location)) {
            LOGGER.info("Config location is empty. Read from default path.");
            return null;
        }
        File file = new File(location);
        LOGGER.info("File absolute path:" + file.getAbsolutePath());
        File[] files = file.listFiles();
        if (files == null) {
            LOGGER.warn("The defaultPath {} does not exist.Please check your config in application.properties.", location);
            throw new FileNotFoundException();
        }
        return getConfPath(name, files, location);
    }

    private static String getConfPath(String name, File[] files, String location) {
        String path = null;
        for (File f : files) {
            if (f.getName().equals(name)) {
                path = location + File.separator + name;
                LOGGER.info("config real path: {}", path);
            }
        }
        return path;
    }

}
