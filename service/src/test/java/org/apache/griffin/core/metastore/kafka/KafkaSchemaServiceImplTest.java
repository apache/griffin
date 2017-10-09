package org.apache.griffin.core.metastore.kafka;///*
//Licensed to the Apache Software Foundation (ASF) under one
//or more contributor license agreements.  See the NOTICE file
//distributed with this work for additional information
//regarding copyright ownership.  The ASF licenses this file
//to you under the Apache License, Version 2.0 (the
//"License"); you may not use this file except in compliance
//with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing,
//software distributed under the License is distributed on an
//"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//KIND, either express or implied.  See the License for the
//specific language governing permissions and limitations
//under the License.
//*/
//
//package org.apache.griffin.core.metastore.kafka;
//
//import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
//import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
//import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mockito;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.TestConfiguration;
//import org.springframework.context.annotation.Bean;
//import org.springframework.test.context.junit4.SpringRunner;
//import org.springframework.web.client.RestTemplate;
//
//import static org.assertj.core.api.Assertions.fail;
//import static org.junit.Assert.assertTrue;
//
//@RunWith(SpringRunner.class)
//public class KafkaSchemaServiceImplTest {
//    @TestConfiguration
//    public static class KafkaSchemaServiceConfiguration {
//        @Bean
//        public KafkaSchemaServiceImpl service() {
//            return new KafkaSchemaServiceImpl();
//        }
//    }
//
//    @Autowired
//    private KafkaSchemaServiceImpl service;
//
//    @Before
//    public void setup(){
//        service.restTemplate= Mockito.mock(RestTemplate.class);
//    }
//
//    @Test
//    public void testGetSchemaString(){
//        try {
//            SchemaString tmp = service.getSchemaString(1);
//            assertTrue(true);
//        }catch (Throwable t){
//            fail("Cannot get all tables from all dbs");
//        }
//    }
//
//    @Test
//    public void testGetSubjects(){
//        try {
//            Iterable<String> tmp = service.getSubjects();
//            assertTrue(true);
//        }catch (Throwable t){
//            fail("Cannot get all tables from all dbs");
//        }
//    }
//
//    @Test
//    public void testGetSubjectVersions(){
//        try {
//            Iterable<Integer> tmp = service.getSubjectVersions("");
//            assertTrue(true);
//        }catch (Throwable t){
//            fail("Cannot get all tables from all dbs");
//        }
//    }
//
//    @Test
//    public void testGetSubjectSchema(){
//        try {
//            Schema tmp = service.getSubjectSchema("","");
//            assertTrue(true);
//        }catch (Throwable t){
//            fail("Cannot get all tables from all dbs");
//        }
//    }
//
//    @Test
//    public void testGetTopLevelConfig(){
//        try {
//            Config tmp = service.getTopLevelConfig();
//            assertTrue(true);
//        }catch (Throwable t){
//            fail("Cannot get all tables from all dbs");
//        }
//    }
//
//    @Test
//    public void testGetSubjectLevelConfig(){
//        try {
//            Config tmp = service.getSubjectLevelConfig("");
//            assertTrue(true);
//        }catch (Throwable t){
//            fail("Cannot get all tables from all dbs");
//        }
//    }
//}
