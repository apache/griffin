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

package org.apache.griffin.core.metastore.hive;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;


@RunWith(SpringRunner.class)
public class HiveMetaStoreServiceImplTest {

    @TestConfiguration
    public static class HiveMetaStoreServiceConfiguration {
        @Bean
        public HiveMetaStoreService service() {
            return new HiveMetaStoreServiceImpl();
        }
    }

    @MockBean
    private HiveMetaStoreClient client;

    @Autowired
    private HiveMetaStoreService service;

    @Before
    public void setup() {

    }

    @Test
    public void testGetAllDatabasesForNormalRun() throws MetaException {
        given(client.getAllDatabases()).willReturn(Arrays.asList("default"));
        assertEquals(service.getAllDatabases().iterator().hasNext(), true);
    }

    @Test
    public void testGetAllDatabasesForMetaException() throws MetaException {
        given(client.getAllDatabases()).willThrow(MetaException.class);
        doNothing().when(client).reconnect();
        service.getAllDatabases();
        assertTrue(service.getAllDatabases() == null);
    }


    @Test
    public void testGetAllTableNamesForNormalRun() throws MetaException {
        String dbName = "default";
        given(client.getAllTables(dbName)).willReturn(Arrays.asList(dbName));
        assertEquals(service.getAllTableNames(dbName).iterator().hasNext(), true);
    }

    @Test
    public void testGetAllTableNamesForMetaException() throws MetaException {
        String dbName = "default";
        given(client.getAllTables(dbName)).willThrow(MetaException.class);
        doNothing().when(client).reconnect();
        assertTrue(service.getAllTableNames(dbName) == null);

    }

    @Test
    public void testGetAllTableByDBNameForNormalRun() throws TException {
        String useDbName = "default";
        String tableName = "table";
        given(client.getAllTables(useDbName)).willReturn(Arrays.asList(tableName));
        given(client.getTable(useDbName, tableName)).willReturn(new Table());
        assertEquals(service.getAllTable(useDbName).size(), 1);
    }

    @Test
    public void testGetAllTableByDBNameForMetaException() throws TException {
        String useDbName = "default";
        given(client.getAllTables(useDbName)).willThrow(MetaException.class);
        doNothing().when(client).reconnect();
        assertEquals(service.getAllTable(useDbName).size(), 0);
    }

    @Test
    public void testGetAllTableForNormalRun() throws TException {
        String useDbName = "default";
        String tableName = "table";
        List<String> databases = Arrays.asList(useDbName);
        given(client.getAllDatabases()).willReturn(databases);
        given(client.getAllTables(databases.iterator().next())).willReturn(Arrays.asList(tableName));
        given(client.getTable(useDbName, tableName)).willReturn(new Table());
        assertEquals(service.getAllTable().size(), 1);
    }

    @Test
    public void testGetAllTableForMetaException1() throws TException {
        String useDbName = "default";
        List<String> databases = Arrays.asList(useDbName);
        given(client.getAllDatabases()).willReturn(databases);
        given(client.getAllTables(useDbName)).willThrow(MetaException.class);
        doNothing().when(client).reconnect();
        assertEquals(service.getAllTable().get(useDbName).size(), 0);
    }

    @Test
    public void testGetAllTableForMetaException2() throws TException {
        given(client.getAllDatabases()).willThrow(MetaException.class);
        doNothing().when(client).reconnect();
        assertEquals(service.getAllTable().size(), 0);
    }

    @Test
    public void testGetTableForNormalRun() throws Exception {
        String dbName = "default";
        String tableName = "tableName";
        given(client.getTable(dbName, tableName)).willReturn(new Table());
        assertTrue(service.getTable(dbName, tableName) != null);
    }

    @Test
    public void testGetTableForException() throws Exception {
        String dbName = "default";
        String tableName = "tableName";
        given(client.getTable(dbName, tableName)).willThrow(Exception.class);
        doNothing().when(client).reconnect();
        assertTrue(service.getTable(dbName, tableName) == null);
    }
}
