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
//import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
//import org.apache.griffin.core.metastore.kafka.KafkaSchemaController;
//import org.apache.griffin.core.metastore.kafka.KafkaSchemaServiceImpl;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.springframework.test.context.junit4.SpringRunner;
//import org.springframework.test.web.servlet.MockMvc;
//import org.springframework.test.web.servlet.setup.MockMvcBuilders;
//
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
//import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
//
//
//@RunWith(SpringRunner.class)
//public class KafkaSchemaControllerTest {
//    private MockMvc mockMvc;
//
//    @Mock
//    KafkaSchemaServiceImpl kafkaSchemaService;
//
//    @InjectMocks
//    private KafkaSchemaController kafkaSchemaController;
//
//    @Before
//    public void setup(){
//        MockitoAnnotations.initMocks(this);
//        this.mockMvc = MockMvcBuilders.standaloneSetup(kafkaSchemaController).build();
//    }
//
//    @Test
//    public void test_getSubjects() throws Exception {
//        int id=1;
//        SchemaString ss = new SchemaString();
//        when(kafkaSchemaService.getSchemaString(id)).thenReturn(ss);
//        mockMvc.perform(get("/metadata/kafka/schema/{id}",id))
//                .andExpect(status().isOk());
//        verify(kafkaSchemaService).getSchemaString(id);
//    }
//
//    @Test
//    public void test_getSchemaString() throws Exception {
//        when(kafkaSchemaService.getSubjects()).thenReturn(null);
//        mockMvc.perform(get("/metadata/kafka/subject"))
//                .andExpect(status().isOk());
//        verify(kafkaSchemaService).getSubjects();
//    }
//
//    @Test
//    public void test_getSubjectVersions() throws Exception {
//        String subject="sss";
//        when(kafkaSchemaService.getSubjectVersions(subject)).thenReturn(null);
//        mockMvc.perform(get("/metadata/kafka/subject/{subject}/version",subject))
//                .andExpect(status().isOk());
//        verify(kafkaSchemaService).getSubjectVersions(subject);
//    }
//
//    @Test
//    public void test_getSubjectSchema() throws Exception {
//        String subject="ss.s";
//        String version="ss";
//        when(kafkaSchemaService.getSubjectSchema(subject, version)).thenReturn(null);
//        mockMvc.perform(get("/metadata/kafka/subject/{subject}/version/{version}",subject,version))
//                .andExpect(status().isOk());
//        verify(kafkaSchemaService).getSubjectSchema(subject, version);
//    }
//
//    @Test
//    public void test_getTopLevelConfig() throws Exception {
//        when(kafkaSchemaService.getTopLevelConfig()).thenReturn(null);
//        mockMvc.perform(get("/metadata/kafka/config"))
//                .andExpect(status().isOk());
//        verify(kafkaSchemaService).getTopLevelConfig();
//    }
//
//    @Test
//    public void test_getSubjectLevelConfig() throws Exception {
//        String subject="sss";
//        when(kafkaSchemaService.getSubjectLevelConfig(subject)).thenReturn(null);
//        mockMvc.perform(get("/metadata/kafka/config/{subject}",subject))
//                .andExpect(status().isOk());
//        verify(kafkaSchemaService).getSubjectLevelConfig(subject);
//    }
//}
