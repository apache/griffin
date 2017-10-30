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

package org.apache.griffin.core.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.metastore.hive.entity.HiveDebugTable;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class GriffinUtilTest {

    @Before
    public void setup() {
    }

    @Test
    public void testToJson() {
        JobHealth jobHealth = new JobHealth(5, 10);
        String jobHealthStr = GriffinUtil.toJson(jobHealth);
        System.out.println(jobHealthStr);
        assertEquals(jobHealthStr, "{\"healthyJobCount\":5,\"jobCount\":10}");
    }

    @Test
    public void testToEntityWithParamClass() throws IOException {
        String str = "{\"healthyJobCount\":5,\"jobCount\":10}";
        JobHealth jobHealth = GriffinUtil.toEntity(str, JobHealth.class);
        assertEquals(jobHealth.getJobCount(), 10);
        assertEquals(jobHealth.getHealthyJobCount(), 5);
    }

    @Test
    public void testToEntityWithParamTypeReference() throws IOException {
        String str = "{\"aaa\":12, \"bbb\":13}";
        TypeReference<HashMap<String, Integer>> type = new TypeReference<HashMap<String, Integer>>() {
        };
        Map map = GriffinUtil.toEntity(str, type);
        assertEquals(map.get("aaa"), 12);
    }

    @Test
    public void testGetPropertiesForSuccess() {
        Properties properties = GriffinUtil.getProperties("/quartz.properties");
        assertEquals(properties.get("org.quartz.jobStore.isClustered"), "true");
    }

    @Test
    public void testGetPropertiesForFailWithWrongPath() {
        Properties properties = GriffinUtil.getProperties(".././quartz.properties");
        assertEquals(properties, null);
    }

    @Test
    public void testToJsonWithFormat() {
        JobHealth jobHealth = new JobHealth(5, 10);
        String jobHealthStr = GriffinUtil.toJsonWithFormat(jobHealth);
        System.out.println(jobHealthStr);
    }

    @Test
    public void testToEntityFromFile() throws Exception {
        Gson gson=new Gson();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ClassPathResource("hive_tables.json").getInputStream()));
        HiveDebugTable table=gson.fromJson(reader,HiveDebugTable.class);
        assertEquals(table.getDbTables().size(),2);
    }
}
