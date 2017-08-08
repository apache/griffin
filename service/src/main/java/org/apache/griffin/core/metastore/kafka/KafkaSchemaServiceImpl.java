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
import org.apache.griffin.core.error.exception.GriffinException.KafkaConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;

@Service
public class KafkaSchemaServiceImpl implements KafkaSchemaService{

    private static final Logger log = LoggerFactory.getLogger(KafkaSchemaServiceImpl.class);

    @Value("${kafka.schema.registry.url}")
    private String url;

    RestTemplate restTemplate = new RestTemplate();

    private String registryUrl(final String path) {
        if (StringUtils.hasText(path)) {
            String usePath = path;
            if (!path.startsWith("/")) usePath = "/" + path;
            return this.url + usePath;
        }
        return "";
    }

    @Override
    public SchemaString getSchemaString(Integer id) {
        String path = "/schemas/ids/" + id;
        String regUrl = registryUrl(path);
        SchemaString result = null;
        try {
            ResponseEntity<SchemaString> res = restTemplate.getForEntity(regUrl, SchemaString.class);
            result = res.getBody();
        } catch (Exception e) {
            log.error("Exception getting schema of id " + id + " : ", e.getMessage());
            throw new KafkaConnectionException();
        }
        return result;
    }

    @Override
    public Iterable<String> getSubjects() {
        String path = "/subjects";
        String regUrl = registryUrl(path);
        Iterable<String> result = null;
        try {
            ResponseEntity<String[]> res = restTemplate.getForEntity(regUrl, String[].class);
            result = Arrays.asList(res.getBody());
        } catch (RestClientException e) {
            log.error("Exception getting subjects : ", e.getMessage());
            throw new KafkaConnectionException();
        }
        return result;
    }

    @Override
    public Iterable<Integer> getSubjectVersions(String subject) {
        String path = "/subjects/" + subject + "/versions";
        String regUrl = registryUrl(path);
        Iterable<Integer> result = null;
        try {
            ResponseEntity<Integer[]> res = restTemplate.getForEntity(regUrl, Integer[].class);
            result = Arrays.asList(res.getBody());
        } catch (Exception e) {
            log.error("Exception getting subject " + subject + " versions : ", e.getMessage());
            throw new KafkaConnectionException();
        }
        return result;
    }

    @Override
    public Schema getSubjectSchema(String subject, String version) {
        String path = "/subjects/" + subject + "/versions/" + version;
        String regUrl = registryUrl(path);
        Schema result = null;
        try {
            ResponseEntity<Schema> res = restTemplate.getForEntity(regUrl, Schema.class);
            result = res.getBody();
        } catch (Exception e) {
            log.error("Exception getting subject " + subject + " with version " + version + " : ", e.getMessage());
            throw new KafkaConnectionException();
        }
        return result;
    }

    @Override
    public Config getTopLevelConfig() {
        String path = "/config";
        String regUrl = registryUrl(path);
        Config result = null;
        try {
            ResponseEntity<Config> res = restTemplate.getForEntity(regUrl, Config.class);
            result = res.getBody();
        } catch (Exception e) {
            log.error("Exception getting top level config : ", e.getMessage());
            throw new KafkaConnectionException();
        }
        return result;
    }

    @Override
    public Config getSubjectLevelConfig(String subject) {
        String path = "/config/" + subject;
        String regUrl = registryUrl(path);
        Config result = null;
        try {
            ResponseEntity<Config> res = restTemplate.getForEntity(regUrl, Config.class);
            result = res.getBody();
        } catch (Exception e) {
            log.error("Exception getting subject " + subject + " level config : ", e.getMessage());
            throw new KafkaConnectionException();
        }
        return result;
    }
}
