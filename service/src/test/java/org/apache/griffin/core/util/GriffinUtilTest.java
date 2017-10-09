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
import org.apache.griffin.core.job.entity.JobHealth;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by xiangrchen on 7/27/17.
 */
public class GriffinUtilTest {

    @Before
    public void setup(){
    }

    @Test
    public void test_toJson(){
        JobHealth jobHealth=new JobHealth(5,10);
        String jobHealthStr= GriffinUtil.toJson(jobHealth);
        assertEquals(jobHealthStr,"{\"healthyJobCount\":5,\"jobCount\":10}");
    }

    @Test
    public void test_toEntity() throws IOException {
        String str="{\"healthyJobCount\":5,\"jobCount\":10}";
        JobHealth jobHealth= GriffinUtil.toEntity(str,JobHealth.class);
        assertEquals(jobHealth.getJobCount(),10);
        assertEquals(jobHealth.getHealthyJobCount(),5);
    }

    @Test
    public void test_toEntity1() throws IOException {
        String str="{\"aaa\":12, \"bbb\":13}";
        TypeReference<HashMap<String,Integer>> type=new TypeReference<HashMap<String,Integer>>(){};
        Map map= GriffinUtil.toEntity(str,type);
        assertEquals(map.get("aaa"),12);
    }

    @Test
    public void test_toJson2() throws IOException {
//        LivySessionStateMap.State state=
        System.out.println(GriffinOperationMessage.CREATE_JOB_FAIL);
    }

}
