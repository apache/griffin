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

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;


@RunWith(SpringRunner.class)
//@TestPropertySource(properties = {"hive.metastore.uris=thrift://10.9.246.187:9083"})
public class HiveMetastoreServiceImplTest {

    @TestConfiguration
    public static class HiveMetastoreServiceConfiguration{
        @Bean
        public HiveMetastoreServiceImpl service(){
            return new HiveMetastoreServiceImpl();
        }
    }

    @MockBean
    private HiveMetaStoreClient client;

    @Autowired
    private HiveMetastoreServiceImpl service;

    @Before
    public void setup(){

    }

    @Test
    public void testGetAllDatabases(){
        try {
            Iterable<String> tmp = service.getAllDatabases();
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables from all dbs");
        }
    }


    @Test
    public void testGetAllTableNames(){
        try {
            Iterable<String> tmp = service.getAllTableNames("default");
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables in db default");
        }
    }

    @Test
    public void testGetAllTableByDBName(){
        try {
            String useDbName="default";
            given(client.getAllTables(useDbName)).willReturn(Arrays.asList("cout","cout1"));
            List<Table> tmp = service.getAllTable(useDbName);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables in default db");
        }
    }

    @Test
    public void testGetAllTable(){
        try {
            Iterable<String> dbs=new ArrayList<>();
            given(service.getAllDatabases()).willReturn(Arrays.asList("default","griffin"));
            String useDbName="default";
            given(client.getAllTables(useDbName)).willReturn(Arrays.asList("cout","cout1"));
            Map<String, List<Table>> tmp = service.getAllTable();
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all table in map format");
        }
    }

    @Test
    public void testGetDesignatedTable(){
        try {
            Table tmp = service.getTable("default","xxx");
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get xxx table in default db");
        }
    }
}
