/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

package org.apache.griffin.core.metastore;

import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;

@Service
public class KafkaSchemaService {

    private static final Logger log = LoggerFactory.getLogger(KafkaSchemaService.class);

    @Value("${kafka.schema.registry.url}")
    private String url;


    private String registryUrl(final String path) {
        if (StringUtils.hasText(path)) {
            String usePath = path;
            if (!path.startsWith("/")) usePath = "/" + path;
            return this.url + usePath;
        }
        return "";
    }

    public SchemaString getSchemaString(Integer id) {
        String path = "/schemas/ids/" + id;
        String regUrl = registryUrl(path);
        SchemaString result = null;
        try {
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<SchemaString> res = restTemplate.getForEntity(regUrl, SchemaString.class);
            result = res.getBody();
        } catch (Exception e) {
            log.error("Exception getting schema of id " + id + " : ", e.getMessage());
        }
        return result;
    }

    public Iterable<String> getSubjects() {
        String path = "/subjects";
        String regUrl = registryUrl(path);
        Iterable<String> result = null;
        try {
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<String[]> res = restTemplate.getForEntity(regUrl, String[].class);
            result = Arrays.asList(res.getBody());
        } catch (Exception e) {
            log.error("Exception getting subjects : ", e.getMessage());
        }
        return result;
    }

    public Iterable<Integer> getSubjectVersions(String subject) {
        String path = "/subjects/" + subject + "/versions";
        String regUrl = registryUrl(path);
        Iterable<Integer> result = null;
        try {
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<Integer[]> res = restTemplate.getForEntity(regUrl, Integer[].class);
            result = Arrays.asList(res.getBody());
        } catch (Exception e) {
            log.error("Exception getting subject " + subject + " versions : ", e.getMessage());
        }
        return result;
    }

    public Schema getSubjectSchema(String subject, String version) {
        String path = "/subjects/" + subject + "/versions/" + version;
        String regUrl = registryUrl(path);
        Schema result = null;
        try {
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<Schema> res = restTemplate.getForEntity(regUrl, Schema.class);
            result = res.getBody();
        } catch (Exception e) {
            log.error("Exception getting subject " + subject + " with version " + version + " : ", e.getMessage());
        }
        return result;
    }

    public Config getTopLevelConfig() {
        String path = "/config";
        String regUrl = registryUrl(path);
        Config result = null;
        try {
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<Config> res = restTemplate.getForEntity(regUrl, Config.class);
            result = res.getBody();
        } catch (Exception e) {
            log.error("Exception getting top level config : ", e.getMessage());
        }
        return result;
    }

    public Config getSubjectLevelConfig(String subject) {
        String path = "/config/" + subject;
        String regUrl = registryUrl(path);
        Config result = null;
        try {
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<Config> res = restTemplate.getForEntity(regUrl, Config.class);
            result = res.getBody();
        } catch (Exception e) {
            log.error("Exception getting subject " + subject + " level config : ", e.getMessage());
        }
        return result;
    }
}
