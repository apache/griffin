package org.apache.griffin.core.metastore;

import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/metadata/kafka")
public class KafkaSchemaController {

    @Autowired
    KafkaSchemaService kafkaSchemaService;

    @RequestMapping("/schema/{id}")
    public SchemaString getSchemaString(@PathVariable("id") Integer id) {
        return kafkaSchemaService.getSchemaString(id);
    }

    @RequestMapping("/subject")
    public Iterable<String> getSubjects() {
        return kafkaSchemaService.getSubjects();
    }

    @RequestMapping("/subject/{subject}/version")
    public Iterable<Integer> getSubjectVersions(@PathVariable("subject") String subject) {
        return kafkaSchemaService.getSubjectVersions(subject);
    }

    @RequestMapping("/subject/{subject}/version/{version}")
    public Schema getSubjectSchema(@PathVariable("subject") String subject, @PathVariable("version") String version) {
        return kafkaSchemaService.getSubjectSchema(subject, version);
    }

    @RequestMapping("/config")
    public Config getTopLevelConfig() {
        return kafkaSchemaService.getTopLevelConfig();
    }

    @RequestMapping("/config/{subject}")
    public Config getSubjectLevelConfig(@PathVariable("subject") String subject) {
        return kafkaSchemaService.getSubjectLevelConfig(subject);
    }

}
