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

package org.apache.griffin.core.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.job.entity.JobDataSegment;
import org.apache.griffin.core.job.entity.JobSchedule;
import org.apache.griffin.core.job.entity.SegmentPredicate;
import org.apache.griffin.core.job.entity.SegmentSplit;
import org.apache.griffin.core.job.repo.JobScheduleRepo;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.griffin.core.util.TimeUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class JobInstance implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobInstance.class);
    public static final String MEASURE_KEY = "measure";
    public static final String PREDICTS_KEY = "predicts";
    public static final String JOB_NAME_KEY = "jobName";
    public static final String GROUP_NAME_KEY = "groupName";
    public static final String DELETED_KEY = "deleted";
    public static final String PATH_CONNECTOR_CHARACTER = ",";

    @Autowired
    private SchedulerFactoryBean factory;
    @Autowired
    private MeasureRepo measureRepo;
    @Autowired
    private JobScheduleRepo jobScheduleRepo;

    private JobSchedule jobSchedule;
    private Measure measure;
    private List<SegmentPredicate> mPredicts;
    private Long jobStartTime;


    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            initParam(context);
            setDataSourcesPartitions(measure.getDataSources());
            createJobInstance(jobSchedule.getConfigMap(), context);
        } catch (Exception e) {
            LOGGER.error("Create job failure.", e);
        }
    }

    private void initParam(JobExecutionContext context) throws SchedulerException {
        mPredicts = new ArrayList<>();
        JobDetail jobDetail = context.getJobDetail();
        Long measureId = jobDetail.getJobDataMap().getLong("measureId");
        Long jobScheduleId = jobDetail.getJobDataMap().getLong("jobScheduleId");
        setJobStartTime(jobDetail);
        measure = measureRepo.findOne(measureId);
        if (measure == null) {
            LOGGER.error("Measure with id {} is not found!", measureId);
            throw new NullPointerException();
        }
        jobSchedule = jobScheduleRepo.findOne(jobScheduleId);
        Long timestampOffset = TimeUtil.str2Long(jobSchedule.getTimestampOffset());
        measure.setTriggerTimeStamp(jobStartTime + timestampOffset);
    }

    private void setJobStartTime(JobDetail jobDetail) throws SchedulerException {
        Scheduler scheduler = factory.getObject();
        JobKey jobKey = jobDetail.getKey();
        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
        Date triggerTime = triggers.get(0).getPreviousFireTime();
        jobStartTime = triggerTime.getTime();
    }


    private void setDataSourcesPartitions(List<DataSource> sources) throws Exception {
        if (CollectionUtils.isEmpty(sources)) {
            throw new NullPointerException("Measure data sources can not be empty.");
        }
        List<JobDataSegment> segments = jobSchedule.getSegments();
        for (JobDataSegment dataSegment : segments) {
            String connectorIndex = dataSegment.getDataConnectorIndex();
            if (connectorIndex == null || !connectorIndex.matches(".+\\[\\d+]")) {
                throw new IllegalArgumentException("Data segments connector index format error.");
            }

            for (DataSource source : sources) {
                setDataSourcePartitions(dataSegment, source);
            }
        }
    }

    private int getIndex(String connectorIndex) {
        Pattern pattern = Pattern.compile("\\[.*]");
        Matcher matcher = pattern.matcher(connectorIndex);
        int index = 0;
        while (matcher.find()) {
            String group = matcher.group();
            group = group.replace("[", "").replace("]", "");
            index = Integer.parseInt(group);
        }
        return index;
    }

    private void setDataSourcePartitions(JobDataSegment dataSegment, DataSource dataSource) throws Exception {
        List<DataConnector> connectors = dataSource.getConnectors();
        if (getIndex(dataSegment.getDataConnectorIndex()) >= connectors.size()) {
            throw new ArrayIndexOutOfBoundsException("Data segments connector index format error.");
        }
        for (int index = 0; index < connectors.size(); index++) {
            setDataConnectorPartitions(dataSegment, dataSource, connectors.get(index), index);
        }
    }


    private void setDataConnectorPartitions(JobDataSegment ds, DataSource source, DataConnector dataConnector, int index) throws Exception {
        if (ds.getDataConnectorIndex().equals(getConnectorIndex(source, index))
                && ds.getSegmentSplit() != null && ds.getConfig() != null) {
            Long[] sampleTimestamps = genSampleTs(ds.getSegmentSplit());
            setDataConnectorConf(dataConnector, ds, sampleTimestamps);
            setSegPredictsConf(ds, sampleTimestamps);
        }
    }

    private String getConnectorIndex(DataSource source, int index) {
        StringBuilder sb = new StringBuilder();
        sb.append(source.getName());
        sb.append("[").append(index).append("]");
        return sb.toString();
    }

    /**
     * split data into several part and get every part start timestamp
     *
     * @param segSplit config of data
     * @return split timestamps of data
     */
    private Long[] genSampleTs(SegmentSplit segSplit) {
        Long offset = TimeUtil.str2Long(segSplit.getOffset());
        Long range = TimeUtil.str2Long(segSplit.getRange());
        Long dataUnit = TimeUtil.str2Long(segSplit.getDataUnit());
        //offset usually is negative
        Long dataStartTime = jobStartTime + offset;
        if (range < 0) {
            dataStartTime += range;
            range = Math.abs(range);
        }
        if (Math.abs(dataUnit) >= range || dataUnit == 0) {
            return new Long[]{dataStartTime};
        }
        int count = (int) (range / dataUnit);
        Long[] timestamps = new Long[count];
        for (int index = 0; index < count; index++) {
            timestamps[index] = dataStartTime + index * dataUnit;
        }
        return timestamps;
    }

    /**
     * set all class SegmentPredicate configs
     *
     * @param segment  job data segment
     * @param sampleTs collection of data split start timestamp
     */
    private void setSegPredictsConf(JobDataSegment segment, Long[] sampleTs) throws IOException {
        List<SegmentPredicate> predicates = segment.getPredicates();
        if (predicates != null) {
            for (SegmentPredicate predicate : predicates) {
                genConfMap(predicate.getConfigMap(), sampleTs);
                //Do not forget to update origin string config
                predicate.setConfigMap(predicate.getConfigMap());
                mPredicts.add(predicate);
            }
        }
    }

    /**
     * set all class SegmentPredicate configs
     *
     * @param segment  job data segment
     * @param sampleTs collection of data split start timestamp
     */
    private void setDataConnectorConf(DataConnector dc, JobDataSegment segment, Long[] sampleTs) throws IOException {
        Map<String, String> segConfMap = genConfMap(segment.getConfigMap(), sampleTs);
        segment.setConfigMap(segment.getConfigMap());
        Map<String, String> confMap = dc.getConfigMap();
        for (Map.Entry<String, String> entry : segConfMap.entrySet()) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        //Do not forget to update data connector String config
        dc.setConfigMap(confMap);
    }


    /**
     * @param conf     map with file predicate,data split and partitions info
     * @param sampleTs collection of data split start timestamp
     * @return all config data combine,like {"where": "year=2017 AND month=11 AND dt=15 AND hour=09,year=2017 AND month=11 AND dt=15 AND hour=10"}
     * or like {"path": "/year=#2017/month=11/dt=15/hour=09/_DONE,/year=#2017/month=11/dt=15/hour=10/_DONE"}
     */
    private Map<String, String> genConfMap(Map<String, String> conf, Long[] sampleTs) {
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            String value = entry.getValue();
            Set<String> set = new HashSet<>();
            for (Long timestamp : sampleTs) {
                set.add(TimeUtil.format(value, timestamp));
            }
            conf.put(entry.getKey(), StringUtils.join(set, ","));
        }
        return conf;
    }

    private boolean createJobInstance(Map<String, String> confMap, JobExecutionContext context) throws Exception {
        if (confMap == null || confMap.get("interval") == null || confMap.get("repeat") == null) {
            throw new NullPointerException("Predicate config is null.");
        }
        Long interval = TimeUtil.str2Long(confMap.get("interval"));
        Integer repeat = Integer.valueOf(confMap.get("repeat"));
        String groupName = "predicate_group";
        String jobName = measure.getName() + "_" + groupName + "_" + System.currentTimeMillis();
        Scheduler scheduler = factory.getObject();
        TriggerKey triggerKey = triggerKey(jobName, groupName);
        return !(scheduler.checkExists(triggerKey) || !createJobInstance(scheduler, triggerKey, interval, repeat, context));
    }


    private boolean createJobInstance(Scheduler scheduler, TriggerKey triggerKey, Long interval, Integer repeatCount, JobExecutionContext context) throws Exception {
        JobDetail jobDetail = addJobDetail(scheduler, triggerKey, context);
        scheduler.scheduleJob(newTriggerInstance(triggerKey, jobDetail, interval, repeatCount));
        return true;
    }


    private Trigger newTriggerInstance(TriggerKey triggerKey, JobDetail jobDetail, Long interval, Integer repeatCount) throws ParseException {
        return newTrigger()
                .withIdentity(triggerKey)
                .forJob(jobDetail)
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInMilliseconds(interval)
                        .withRepeatCount(repeatCount)
                )
                .build();
    }

    private JobDetail addJobDetail(Scheduler scheduler, TriggerKey triggerKey, JobExecutionContext context) throws SchedulerException, JsonProcessingException {
        JobKey jobKey = jobKey(triggerKey.getName(), triggerKey.getGroup());
        JobDetail jobDetail;
        Boolean isJobKeyExist = scheduler.checkExists(jobKey);
        if (isJobKeyExist) {
            jobDetail = scheduler.getJobDetail(jobKey);
        } else {
            jobDetail = newJob(SparkSubmitJob.class)
                    .storeDurably()
                    .withIdentity(jobKey)
                    .build();
        }
        setJobDataMap(jobDetail, context);
        scheduler.addJob(jobDetail, isJobKeyExist);
        return jobDetail;
    }

    private void setJobDataMap(JobDetail jobDetail, JobExecutionContext context) throws JsonProcessingException {
        jobDetail.getJobDataMap().put(MEASURE_KEY, JsonUtil.toJson(measure));
        jobDetail.getJobDataMap().put(PREDICTS_KEY, JsonUtil.toJson(mPredicts));
        jobDetail.getJobDataMap().put(JOB_NAME_KEY, context.getJobDetail().getKey().getName());
        jobDetail.getJobDataMap().put(GROUP_NAME_KEY, context.getJobDetail().getKey().getGroup());
        jobDetail.getJobDataMap().putAsString(DELETED_KEY, false);
    }

}
