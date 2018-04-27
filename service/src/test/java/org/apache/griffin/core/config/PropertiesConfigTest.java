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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.FileNotFoundException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
public class PropertiesConfigTest {

    @TestConfiguration
    public static class PropertiesConf {

        @Bean(name = "noLivyConf")
        public PropertiesConfig noSparkConf() {
            return new PropertiesConfig(null);
        }

        @Bean(name = "livyConf")
        public PropertiesConfig sparkConf() {
            return new PropertiesConfig("src/test/resources");
        }

        @Bean(name = "livyNotFoundConfig")
        public PropertiesConfig sparkNotFoundConfig() {
            return new PropertiesConfig("test");
        }

        @Bean(name = "noQuartzConf")
        public PropertiesConfig noQuartzConf() {
            return new PropertiesConfig(null);
        }

        @Bean(name = "quartzConf")
        public PropertiesConfig quartzConf() {
            return new PropertiesConfig("src/test/resources");
        }

        @Bean(name = "quartzNotFoundConfig")
        public PropertiesConfig quartzNotFoundConfig() {
            return new PropertiesConfig("test");
        }
    }

    @Autowired
    @Qualifier(value = "noLivyConf")
    private PropertiesConfig noLivyConf;

    @Autowired
    @Qualifier(value = "livyConf")
    private PropertiesConfig livyConf;

    @Autowired
    @Qualifier(value = "livyNotFoundConfig")
    private PropertiesConfig livyNotFoundConfig;


    @Autowired
    @Qualifier(value = "noQuartzConf")
    private PropertiesConfig noQuartzConf;

    @Autowired
    @Qualifier(value = "quartzConf")
    private PropertiesConfig quartzConf;

    @Autowired
    @Qualifier(value = "quartzNotFoundConfig")
    private PropertiesConfig quartzNotFoundConfig;

    @Test
    public void appConf() {
        Properties conf = noLivyConf.appConf();
        assertEquals(conf.get("spring.datasource.username"), "test");
    }

    @Test
    public void livyConfWithLocationNotNull() throws Exception {
        Properties conf = livyConf.livyConf();
        assertEquals(conf.get("sparkJob.name"), "test");
    }

    @Test
    public void livyConfWithLocationNull() throws Exception {
        Properties conf = noLivyConf.livyConf();
        assertEquals(conf.get("sparkJob.name"), "test");
    }


    @Test
    public void quartzConfWithLocationNotNull() throws Exception {
        Properties conf = quartzConf.quartzConf();
        assertEquals(conf.get("org.quartz.scheduler.instanceName"), "spring-boot-quartz-test");
    }

    @Test
    public void quartzConfWithLocationNull() throws Exception {
        Properties conf = noQuartzConf.quartzConf();
        assertEquals(conf.get("org.quartz.scheduler.instanceName"), "spring-boot-quartz-test");
    }
}