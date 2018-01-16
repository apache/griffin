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


import org.apache.griffin.core.measure.entity.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.quartz.JobDataMap;
import org.quartz.Trigger;
import org.quartz.impl.JobDetailImpl;

import java.io.Serializable;
import java.util.*;

public class EntityHelper {
    public static GriffinMeasure createATestGriffinMeasure(String name, String org) throws Exception {
        HashMap<String, String> configMap1 = new HashMap<>();
        configMap1.put("database", "default");
        configMap1.put("table.name", "test_data_src");
        HashMap<String, String> configMap2 = new HashMap<>();
        configMap2.put("database", "default");
        configMap2.put("table.name", "test_data_tgt");
        String configJson1 = new ObjectMapper().writeValueAsString(configMap1);
        String configJson2 = new ObjectMapper().writeValueAsString(configMap2);

        DataSource dataSource = new DataSource("source", Arrays.asList(new DataConnector("source_name", "HIVE", "1.2", configJson1)));
        DataSource targetSource = new DataSource("target", Arrays.asList(new DataConnector("target-name", "HIVE", "1.2", configJson2)));

        List<DataSource> dataSources = new ArrayList<>();
        dataSources.add(dataSource);
        dataSources.add(targetSource);
        String rules = "source.id=target.id AND source.name=target.name AND source.age=target.age";
        Map<String, Object> map = new HashMap<>();
        map.put("detail", "detail info");
        Rule rule = new Rule("griffin-dsl", "accuracy", rules, map);
        EvaluateRule evaluateRule = new EvaluateRule(Arrays.asList(rule));
        return new GriffinMeasure(1L,name, "description", org, "batch", "test", dataSources, evaluateRule);
    }

    public static JobDetailImpl createJobDetail() {
        JobDetailImpl jobDetail = new JobDetailImpl();
        JobDataMap jobInfoMap = new JobDataMap();
        jobInfoMap.put("triggerState", Trigger.TriggerState.NORMAL);
        jobInfoMap.put("measureId", "1");
        jobInfoMap.put("sourcePattern", "YYYYMMdd-HH");
        jobInfoMap.put("targetPattern", "YYYYMMdd-HH");
        jobInfoMap.put("jobStartTime", "1506356105876");
        jobInfoMap.put("interval", "3000");
        jobInfoMap.put("deleted", "false");
        jobInfoMap.put("blockStartTimestamp", "1506634804254");
        jobInfoMap.put("lastBlockStartTimestamp", "1506634804254");
        jobInfoMap.put("groupName", "BA");
        jobInfoMap.put("jobName", "jobName");
        jobDetail.setJobDataMap(jobInfoMap);
        return jobDetail;
    }

    public static Map<String, Object> createJobDetailMap() {
        Map<String, Object> jobDetailMap = new HashMap<>();
        jobDetailMap.put("jobName", "jobName");
        jobDetailMap.put("measureId", "1");
        jobDetailMap.put("groupName", "BA");
        jobDetailMap.put("targetPattern", "YYYYMMdd-HH");
        jobDetailMap.put("triggerState", Trigger.TriggerState.NORMAL);
        jobDetailMap.put("nextFireTime", "1509613440000");
        jobDetailMap.put("previousFireTime", "1509613410000");
        jobDetailMap.put("interval", "3000");
        jobDetailMap.put("sourcePattern", "YYYYMMdd-HH");
        jobDetailMap.put("jobStartTime", "1506356105876");
        return jobDetailMap;
    }
}
