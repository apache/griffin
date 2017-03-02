package org.apache.griffin.core.metastore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

/**
 * Created by lliu13 on 2017/3/2.
 */


public class KafkaSchemaRegistryTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistryTest.class);

    public static void main(String[] args) {
        final String uri = "http://10.65.159.119:8081/subjects";

        RestTemplate restTemplate = new RestTemplate();
        String result = restTemplate.getForObject(uri, String.class);

        log.info(result);
    }
}
