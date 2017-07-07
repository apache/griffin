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

package org.apache.griffin.core.metastore;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
public class HiveMetastoreControllerTest {
    private MockMvc mockMvc;

    @Mock
    HiveMetastoreServiceImpl hiveMetastoreService;

    @InjectMocks
    private HiveMetastoreController hiveMetastoreController;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.standaloneSetup(hiveMetastoreController).build();
    }

    @Test
    public void test_getAllDatabases() throws Exception {
        when(hiveMetastoreService.getAllDatabases()).thenReturn(null);
        mockMvc.perform(get("/metadata/hive/db"))
                .andExpect(status().isOk());
        verify(hiveMetastoreService).getAllDatabases();
    }

    @Test
    public void test_getDefAllTables() throws Exception{
        when(hiveMetastoreService.getAllTableNames("")).thenReturn(null);
        mockMvc.perform(get("/metadata/hive/table"))
                .andExpect(status().isOk());
        verify(hiveMetastoreService).getAllTableNames("");
    }

    @Test
    public void test_getAllTableNamess() throws Exception {
        String db="default";
        when(hiveMetastoreService.getAllTableNames(db)).thenReturn(null);
        mockMvc.perform(get("/metadata/hive/{db}/table",db))
                .andExpect(status().isOk());
        verify(hiveMetastoreService).getAllTableNames(db);
    }

    @Test
    public void test_getAllTables() throws Exception {
        String db="default";
        when(hiveMetastoreService.getAllTable(db)).thenReturn(null);
        mockMvc.perform(get("/metadata/hive/{db}/alltables",db))
                .andExpect(status().isOk());
        verify(hiveMetastoreService).getAllTable(db);
    }

    @Test
    public void test_getAllTables2() throws Exception {
        when(hiveMetastoreService.getAllTable()).thenReturn(null);
        mockMvc.perform(get("/metadata/hive/alltables"))
                .andExpect(status().isOk());
        verify(hiveMetastoreService).getAllTable();
    }

    @Test
    public void test_getDefTable() throws Exception {
        String dbName="";
        String tableName="cout";
        when(hiveMetastoreService.getTable(dbName,tableName)).thenReturn(null);
        mockMvc.perform(get("/metadata/hive/table/{table}",tableName))
                .andExpect(status().isOk());
        verify(hiveMetastoreService).getTable(dbName,tableName);
    }

    @Test
    public void test_getTable() throws Exception{
        String db="default";
        String table="cout";
        when(hiveMetastoreService.getTable(db,table)).thenReturn(null);
        mockMvc.perform(get("/metadata/hive/{db}/table/{table}",db,table))
                .andExpect(status().isOk());
        verify(hiveMetastoreService).getTable(db,table);
    }
}
