package org.apache.griffin.core.measure;


import org.apache.griffin.core.measure.entity.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.quartz.JobDataMap;
import org.quartz.Trigger;
import org.quartz.impl.JobDetailImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MeasureTestHelper {
    public static Measure createATestMeasure(String name, String org) throws Exception{
        HashMap<String, String> configMap1 = new HashMap<>();
        configMap1.put("database", "default");
        configMap1.put("table.name", "test_data_src");
        HashMap<String, String> configMap2 = new HashMap<>();
        configMap2.put("database", "default");
        configMap2.put("table.name", "test_data_tgt");
        String configJson1 = new ObjectMapper().writeValueAsString(configMap1);
        String configJson2 = new ObjectMapper().writeValueAsString(configMap2);

        DataSource dataSource = new DataSource("source", Arrays.asList(new DataConnector("HIVE", "1.2", configJson1)));
        DataSource targetSource = new DataSource("target", Arrays.asList(new DataConnector("HIVE", "1.2", configJson2)));

        List<DataSource> dataSources = new ArrayList<>();
        dataSources.add(dataSource);
        dataSources.add(targetSource);
        String rules = "source.id=target.id AND source.name=target.name AND source.age=target.age";
        Rule rule = new Rule("griffin-dsl", "accuracy", rules);
        EvaluateRule evaluateRule = new EvaluateRule(Arrays.asList(rule));
        return new Measure(name, "description", org, "batch", "test", dataSources, evaluateRule);
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
        jobInfoMap.put("blockStartTimestamp","1506634804254");
        jobInfoMap.put("lastBlockStartTimestamp","1506634804254");
        jobInfoMap.put("groupName","BA");
        jobInfoMap.put("jobName","jobName");
        jobDetail.setJobDataMap(jobInfoMap);
        return jobDetail;
    }
}
