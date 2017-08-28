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

package org.apache.griffin.core.metastore.kafka;

import com.sun.jersey.client.urlconnection.HTTPSProperties;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@PropertySource("classpath:application.properties")
public class KafkaSchemaServiceImplTest {
    /*@TestConfiguration
    public static class KafkaSchemaServiceConfiguration {
        @Bean
        public KafkaSchemaServiceImpl service() {
            return new KafkaSchemaServiceImpl();
        }
    }*/

    @InjectMocks
    private KafkaSchemaServiceImpl service;

    @Mock
    private RestTemplate restTemplate;


    @Before
    public void setup() throws IOException {
/*
        Properties sparkJobProperties=new Properties();
        sparkJobProperties.load(new FileInputStream(new ClassPathResource("sparkJob.properties").getFile()));
        ReflectionTestUtils.setField(service, "url", sparkJobProperties.getProperty("kafka.schema.registry.url"));
*/
    }

    @Test
    public void testGetSchemaString(){
        try {
            String regUrl="null/schemas/ids/1";
            when(restTemplate.getForEntity(regUrl, SchemaString.class)).thenReturn(new ResponseEntity<SchemaString>(new SchemaString(), HttpStatus.OK));
            SchemaString tmp = service.getSchemaString(1);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables from all dbs");
        }
    }

    @Test
    public void testGetSubjects(){
        try {
            String regUrl="null/subjects";
            when(restTemplate.getForEntity(regUrl, String[].class)).thenReturn(new ResponseEntity<String[]>(new String[2], HttpStatus.OK));
            Iterable<String> tmp = service.getSubjects();
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables from all dbs");
        }
    }

    @Test
    public void testGetSubjectVersions(){
        try {
            String regUrl="null/subjects/1.0/versions";
            when(restTemplate.getForEntity(regUrl, Integer[].class)).thenReturn(new ResponseEntity<Integer[]>(new Integer[2], HttpStatus.OK));
            Iterable<Integer> tmp = service.getSubjectVersions("1.0");
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables from all dbs");
        }
    }

    @Test
    public void testGetSubjectSchema(){
        try {
            String regUrl="null/subjects/subject1/versions/version1";
            when(restTemplate.getForEntity(regUrl, Schema.class)).thenReturn(new ResponseEntity<Schema>(new Schema("",0,0, ""), HttpStatus.OK));
            Schema tmp = service.getSubjectSchema("subject1","version1");
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables from all dbs");
        }
    }

    @Test
    public void testGetTopLevelConfig(){
        try {
            String regUrl="null/config";
            when(restTemplate.getForEntity(regUrl, Config.class)).thenReturn(new ResponseEntity<Config>(new Config(), HttpStatus.OK));
            Config tmp = service.getTopLevelConfig();
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables from all dbs");
        }
    }

    @Test
    public void testGetSubjectLevelConfig(){
        try {
            String regUrl="null/config/";
            when(restTemplate.getForEntity(regUrl, Config.class)).thenReturn(new ResponseEntity<Config>(new Config(), HttpStatus.OK));
            Config tmp = service.getSubjectLevelConfig("");
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables from all dbs");
        }
    }
}
