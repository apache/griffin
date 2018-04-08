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


import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.measure.entity.*;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.SimpleTrigger;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;

import java.io.IOException;
import java.util.*;

import static org.apache.griffin.core.job.JobInstance.*;
import static org.apache.griffin.core.job.JobServiceImpl.GRIFFIN_JOB_ID;
import static org.apache.griffin.core.job.JobServiceImpl.JOB_SCHEDULE_ID;
import static org.apache.hadoop.mapreduce.MRJobConfig.JOB_NAME;

public class EntityHelper {
    public static GriffinMeasure createGriffinMeasure(String name) throws Exception {
        DataConnector dcSource = createDataConnector("source_name", "default", "test_data_src", "dt=#YYYYMMdd# AND hour=#HH#");
        DataConnector dcTarget = createDataConnector("target_name", "default", "test_data_tgt", "dt=#YYYYMMdd# AND hour=#HH#");
        return createGriffinMeasure(name, dcSource, dcTarget);
    }

    public static GriffinMeasure createGriffinMeasure(String name, SegmentPredicate srcPredicate, SegmentPredicate tgtPredicate) throws Exception {
        DataConnector dcSource = createDataConnector("source_name", "default", "test_data_src", "dt=#YYYYMMdd# AND hour=#HH#", srcPredicate);
        DataConnector dcTarget = createDataConnector("target_name", "default", "test_data_tgt", "dt=#YYYYMMdd# AND hour=#HH#", tgtPredicate);
        return createGriffinMeasure(name, dcSource, dcTarget);
    }

    public static GriffinMeasure createGriffinMeasure(String name, DataConnector dcSource, DataConnector dcTarget) throws Exception {
        DataSource dataSource = new DataSource("source", Arrays.asList(dcSource));
        DataSource targetSource = new DataSource("target", Arrays.asList(dcTarget));
        List<DataSource> dataSources = new ArrayList<>();
        dataSources.add(dataSource);
        dataSources.add(targetSource);
        String rules = "source.id=target.id AND source.name=target.name AND source.age=target.age";
        Map<String, Object> map = new HashMap<>();
        map.put("detail", "detail info");
        Rule rule = new Rule("griffin-dsl", "accuracy", rules, map);
        EvaluateRule evaluateRule = new EvaluateRule(Arrays.asList(rule));
        return new GriffinMeasure(name, "test", dataSources, evaluateRule);
    }

    public static DataConnector createDataConnector(String name, String database, String table, String where) {
        HashMap<String, String> config = new HashMap<>();
        config.put("database", database);
        config.put("table.name", table);
        config.put("where", where);
        return new DataConnector(name, "1h", config, new ArrayList<>());
    }

    public static DataConnector createDataConnector(String name, String database, String table, String where, SegmentPredicate predicate)  {
        HashMap<String, String> config = new HashMap<>();
        config.put("database", database);
        config.put("table.name", table);
        config.put("where", where);
        return new DataConnector(name, "1h", config, Arrays.asList(predicate));
    }

    public static ExternalMeasure createExternalMeasure(String name) {
        return new ExternalMeasure(name, "description", "org", "test", "metricName", new VirtualJob());
    }

    public static JobSchedule createJobSchedule() throws JsonProcessingException {
        return createJobSchedule("jobName");
    }

    public static JobSchedule createJobSchedule(String jobName) throws JsonProcessingException {
        JobDataSegment segment1 = createJobDataSegment("source_name", true);
        JobDataSegment segment2 = createJobDataSegment("target_name", false);
        List<JobDataSegment> segments = new ArrayList<>();
        segments.add(segment1);
        segments.add(segment2);
        return new JobSchedule(1L, jobName, "0 0/4 * * * ?", "GMT+8:00", segments);
    }

    public static JobSchedule createJobSchedule(String jobName, SegmentRange range) throws JsonProcessingException {
        JobDataSegment segment1 = createJobDataSegment("source_name", true, range);
        JobDataSegment segment2 = createJobDataSegment("target_name", false, range);
        List<JobDataSegment> segments = new ArrayList<>();
        segments.add(segment1);
        segments.add(segment2);
        return new JobSchedule(1L, jobName, "0 0/4 * * * ?", "GMT+8:00", segments);
    }

    public static JobSchedule createJobSchedule(String jobName, JobDataSegment source, JobDataSegment target) throws JsonProcessingException {
        List<JobDataSegment> segments = new ArrayList<>();
        segments.add(source);
        segments.add(target);
        return new JobSchedule(1L, jobName, "0 0/4 * * * ?", "GMT+8:00", segments);
    }

    public static JobDataSegment createJobDataSegment(String dataConnectorName, Boolean baseline, SegmentRange range) {
        return new JobDataSegment(dataConnectorName, baseline, range);
    }

    public static JobDataSegment createJobDataSegment(String dataConnectorName, Boolean baseline) {
        return new JobDataSegment(dataConnectorName, baseline);
    }

    public static JobInstanceBean createJobInstance() {
        JobInstanceBean jobBean = new JobInstanceBean();
        jobBean.setSessionId(1L);
        jobBean.setState(LivySessionStates.State.starting);
        jobBean.setAppId("app_id");
        jobBean.setTms(System.currentTimeMillis());
        return jobBean;
    }

    public static JobDetailImpl createJobDetail(String measureJson, String predicatesJson) {
        JobDetailImpl jobDetail = new JobDetailImpl();
        JobKey jobKey = new JobKey("name", "group");
        jobDetail.setKey(jobKey);
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(MEASURE_KEY, measureJson);
        jobDataMap.put(PREDICATES_KEY, predicatesJson);
        jobDataMap.put(JOB_NAME, "jobName");
        jobDataMap.put(PREDICATE_JOB_NAME, "predicateJobName");
        jobDataMap.put(JOB_SCHEDULE_ID, 1L);
        jobDataMap.put(GRIFFIN_JOB_ID, 1L);
        jobDetail.setJobDataMap(jobDataMap);
        return jobDetail;
    }

    public static SegmentPredicate createFileExistPredicate() throws JsonProcessingException {
        Map<String, String> config = new HashMap<>();
        config.put("root.path", "hdfs:///griffin/demo_src");
        config.put("path", "/dt=#YYYYMMdd#/hour=#HH#/_DONE");
        return new SegmentPredicate("file.exist", config);
    }

    public static Map<String, Object> createJobDetailMap() {
        Map<String, Object> detail = new HashMap<>();
        detail.put("jobId", 1L);
        detail.put("jobName", "jobName");
        detail.put("measureId", 1L);
        detail.put("cronExpression", "0 0/4 * * * ?");
        return detail;
    }

    public static SimpleTrigger createSimpleTrigger(int repeatCount, int triggerCount) {
        SimpleTriggerImpl trigger = new SimpleTriggerImpl();
        trigger.setRepeatCount(repeatCount);
        trigger.setTimesTriggered(triggerCount);
        trigger.setPreviousFireTime(new Date());
        return trigger;
    }

    public static GriffinJob createGriffinJob() {
        return new GriffinJob(1L, 1L, "jobName",
                "quartzJobName", "quartzGroupName", false);
    }

}
