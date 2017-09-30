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

import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/metadata/kafka")
public class KafkaSchemaController {
    //TODO renamed to KafkaProxyController, ***Service
    @Autowired
    KafkaSchemaServiceImpl kafkaSchemaService;

//    @RequestMapping(value = "/schema/{id}",method = RequestMethod.GET)
//    public SchemaString getSchemaString(@PathVariable("id") Integer id) {
//        return kafkaSchemaService.getSchemaString(id);
//    }
//
//    @RequestMapping(value = "/subject",method = RequestMethod.GET)
//    public Iterable<String> getSubjects() {
//        return kafkaSchemaService.getSubjects();
//    }

    @RequestMapping(value = "/topics", method = RequestMethod.GET)
    public Iterable<String> getTopics(){
        return kafkaSchemaService.getTopics();
    }
//    @RequestMapping(value = "/versions",method = RequestMethod.GET)
//    public Iterable<Integer> getSubjectVersions(@RequestParam("subject") String subject) {
//        return kafkaSchemaService.getSubjectVersions(subject);
//    }
//
//    @RequestMapping(value = "/subjectSchema",method = RequestMethod.GET)
//    public Schema getSubjectSchema(@RequestParam("subject") String subject, @RequestParam("version") String version) {
//        return kafkaSchemaService.getSubjectSchema(subject, version);
//    }
//
//    @RequestMapping(value = "/config",method = RequestMethod.GET)
//    public Config getTopLevelConfig() {
//        return kafkaSchemaService.getTopLevelConfig();
//    }
//
//    @RequestMapping(value = "/config/{subject}",method = RequestMethod.GET)
//    public Config getSubjectLevelConfig(@PathVariable("subject") String subject) {
//        return kafkaSchemaService.getSubjectLevelConfig(subject);
//    }

}
