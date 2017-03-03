package org.apache.griffin.core.metastore;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;

/**
 * Created by lliu13 on 2017/3/2.
 */


public class KafkaSchemaRegistryTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistryTest.class);

    public static void main(String[] args) {
//        final String uri = "http://10.65.159.119:8081/subjects";
//        final String uri = "http://10.65.159.119:8081/subjects/Kafka-value/versions/1";
        final String uri = "http://10.65.159.119:8081/subjects/test1-value/versions/latest";

        RestTemplate restTemplate = new RestTemplate();

//        HttpHeaders headers = new HttpHeaders();
//        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
//        HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);

//        ResponseEntity<Schema> result = restTemplate.exchange(uri, HttpMethod.POST, entity, Schema.class);

//        ResponseEntity<SchemaString[]> result = restTemplate.getForEntity(uri, SchemaString[].class);
        ResponseEntity<Schema> result = restTemplate.getForEntity(uri, Schema.class);

        Schema schema = result.getBody();

        log.info(schema.toString());



//        String result = restTemplate.getForObject(uri, String.class);

//        List<SchemaString> list = Arrays.asList(result.getBody());
//        List<SchemaString> list = Arrays.asList(result.getBody());

//        for (SchemaString s : list) {
//            log.info(s.getSchemaString());
//        }

        org.apache.avro.Schema sc = schemaOf(schema.getSchema());
        log.info(sc.toString());

    }

    private static org.apache.avro.Schema schemaOf(String schema) {
        return new org.apache.avro.Schema.Parser().parse(schema);
    }
}
