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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@TestPropertySource(properties = {"kafka.schema.registry.url = http://10.65.159.119:8081"})
public class KafkaSchemaServiceImplTest {
    @TestConfiguration
    public static class KafkaSchemaServiceConfiguration {
        @Bean
        public KafkaSchemaServiceImpl service() {
            return new KafkaSchemaServiceImpl();
        }

        @Value("${kafka.schema.registry.url}")
        String urls;
    }

    @Autowired
    private KafkaSchemaServiceImpl service;

    @Before
    public void setup(){

    }

    @Test
    public void testGetSchemaString(){
//        try {
//            SchemaString tmp = service.getSchemaString(1);
//            assertTrue(true);
//        }catch (Throwable t){
//            fail("Cannot get all tables from all dbs");
//        }
    }


}
