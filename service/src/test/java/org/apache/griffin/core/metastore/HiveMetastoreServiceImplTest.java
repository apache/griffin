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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@TestPropertySource(properties = {"hive.metastore.uris=thrift://10.9.246.187:9083"})
public class HiveMetastoreServiceImplTest {

    @TestConfiguration
    public static class HiveMetastoreServiceConfiguration{
        @Bean
        public HiveMetastoreServiceImpl service(){
            return new HiveMetastoreServiceImpl();
        }

        @Value("${hive.metastore.uris}")
        String urls;
        @Bean
        public HiveMetaStoreClient client(){

                HiveMetaStoreClient client = null;
                HiveConf hiveConf = new HiveConf();
                hiveConf.set("hive.metastore.local", "false");
                hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
                hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, urls);
                try {
                    client= new HiveMetaStoreClient(hiveConf);
                } catch (MetaException e) {
                    client = null;
                }

                return client;

        }
    }
    @Autowired private HiveMetastoreServiceImpl service;


    @Before
    public void setup(){

    }

    @Test
    public void testGetAllTables(){

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
            List<Table> tmp = service.getAllTable("default");
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables in default db");
        }
    }

    @Test
    public void testGetAllTable(){
        try {
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
