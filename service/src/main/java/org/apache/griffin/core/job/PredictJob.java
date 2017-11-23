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
import org.apache.griffin.core.job.entity.JobDataSegment;
import org.apache.griffin.core.job.entity.JobSchedule;
import org.apache.griffin.core.job.entity.SegmentPredict;
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
public class PredictJob implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(PredictJob.class);
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
    private List<SegmentPredict> mPredicts;
    private Long jobStartTime;


    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            initParam(context);
            setDataSourcesPartitions(measure.getDataSources());
            newPredictJob(interval(jobSchedule.getConfigMap()), Long.valueOf(jobSchedule.getConfigMap().get("repeat")), context);
        } catch (Exception e) {
            LOGGER.error("Create job failure.", e);
        }
    }

    private Long interval(Map<String, String> confMap) {
        if (confMap != null && confMap.containsKey("interval")) {
            String interval = confMap.get("interval");
            return TimeUtil.timeString2Long(interval);
        }
        return null;
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
        measure.setTriggerTimeStamp(jobStartTime);
        jobSchedule = jobScheduleRepo.findOne(jobScheduleId);
    }

    private void setJobStartTime(JobDetail jobDetail) throws SchedulerException {
        Scheduler scheduler = factory.getObject();
        JobKey jobKey = jobDetail.getKey();
        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
        Date triggerTime = triggers.get(0).getPreviousFireTime();
        jobStartTime = triggerTime.getTime();
    }


    private void setDataSourcesPartitions(List<DataSource> sources) throws Exception {
        if (sources == null || sources.size() == 0) {
            LOGGER.error("Measure data sources can not be empty.");
            return;
        }
        List<JobDataSegment> segments = jobSchedule.getSegments();
        for (JobDataSegment dataSegment : segments) {
            String connectorIndex = dataSegment.getDataConnectorIndex();
            if (connectorIndex == null || !connectorIndex.matches("(source|target)\\[\\d+]")) {
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
        if (connectors == null || connectors.size() == 0) {
            LOGGER.error("Measure data connector can not be empty.");
            return;
        }
        if (getIndex(dataSegment.getDataConnectorIndex()) >= connectors.size()) {
            throw new ArrayIndexOutOfBoundsException("Data segments connector index format error.");
        }
        for (int index = 0; index < connectors.size(); index++) {
            setDataConnectorPartitions(dataSegment, dataSource, connectors.get(index), index);
        }
    }


    private void setDataConnectorPartitions(JobDataSegment dataSegment, DataSource source, DataConnector dataConnector, int index) throws Exception {
////        JobDataSegment segment = findSegmentOfDataConnector(segments, dataConnector.getId());
        if (dataSegment.getDataConnectorIndex().equals(getMeasureConnectorIndex(source, index))
                && dataSegment.getSegmentSplit() != null && dataSegment.getConfig() != null) {
            Long[] sampleTimestamps = genSampleTimestamps(dataSegment.getSegmentSplit());
            setDataConnectorConf(dataConnector, dataSegment, sampleTimestamps);
            setSegmentPredictsConf(dataSegment, sampleTimestamps);
        }
    }

    private String getMeasureConnectorIndex(DataSource source, int index) {
        StringBuilder sb = new StringBuilder();
        sb.append(source.getName());
        sb.append("[").append(index).append("]");
        return sb.toString();
    }

    /**
     * split data into several part and get every part start timestamp
     *
     * @param segmentSplit config of data
     * @return split timestamps of data
     */
    private Long[] genSampleTimestamps(SegmentSplit segmentSplit) {
        Long offset = TimeUtil.timeString2Long(segmentSplit.getOffset());
        Long range = TimeUtil.timeString2Long(segmentSplit.getRange());
        Long dataUnit = TimeUtil.timeString2Long(segmentSplit.getDataUnit());
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
     * set all class SegmentPredict configs
     *
     * @param segment          job data segment
     * @param sampleTimestamps collection of data split start timestamp
     */
    private void setSegmentPredictsConf(JobDataSegment segment, Long[] sampleTimestamps) throws IOException {
        List<SegmentPredict> predicts = segment.getPredicts();
        if (predicts != null) {
            for (SegmentPredict predict : predicts) {
                genConfMap(predict.getConfigMap(), sampleTimestamps);
                //Do not forget to update origin string config
                predict.setConfig(predict.getConfigMap());
                mPredicts.add(predict);
            }
        }
    }

    /**
     * set all class SegmentPredict configs
     *
     * @param segment          job data segment
     * @param sampleTimestamps collection of data split start timestamp
     */
    private void setDataConnectorConf(DataConnector dataConnector, JobDataSegment segment, Long[] sampleTimestamps) throws IOException {
        Map<String, String> segmentConfMap = genConfMap(segment.getConfigMap(), sampleTimestamps);
        segment.setConfig(segment.getConfigMap());
        Map<String, String> confMap = dataConnector.getConfigMap();
        for (Map.Entry<String, String> entry : segmentConfMap.entrySet()) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        //Do not forget to update data connector String config
        dataConnector.setConfig(confMap);
    }

    private JobDataSegment findSegmentOfDataConnector(List<JobDataSegment> segments, Long dataConnectorId) {
        if (segments == null || segments.size() == 0) {
            return null;
        }
        for (JobDataSegment segment : segments) {
            if (dataConnectorId.equals(segment.getDataConnectorId())) {
                return segment;
            }
        }
        return null;
    }

    /**
     * @param conf             map with file predict,data split and partitions info
     * @param sampleTimestamps collection of data split start timestamp
     * @return all config data combine,like {"where": "year=2017 AND month=11 AND dt=15 AND hour=09,year=2017 AND month=11 AND dt=15 AND hour=10"}
     *          or like
     */
    private Map<String, String> genConfMap(Map<String, String> conf, Long[] sampleTimestamps) {
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            String value = entry.getValue();
            Set<String> set = new HashSet<>();
            for (Long timestamp : sampleTimestamps) {
                set.add(TimeUtil.replaceTimeFormat(value, timestamp));
            }
            conf.put(entry.getKey(), set2String(set));
        }
        return conf;
    }

    private String set2String(Set<String> set) {
        Iterator<String> it = set.iterator();
        StringBuilder sb = new StringBuilder();
        if (!it.hasNext()) {
            return null;
        }
        for (; ; ) {
            sb.append(it.next());
            if (!it.hasNext()) {
                return sb.toString();
            }
            sb.append(PATH_CONNECTOR_CHARACTER);
        }

    }

    public boolean newPredictJob(Long interval, Long repeatCount, JobExecutionContext context) {
        if (interval == null || repeatCount == null) {
            return false;
        }
        String groupName = "predict_group";
        String jobName = measure.getName() + "_" + groupName + "_" + System.currentTimeMillis();
        Scheduler scheduler = factory.getObject();
        TriggerKey triggerKey = triggerKey(jobName, groupName);
        if (isTriggerKeyExist(scheduler, jobName, groupName, triggerKey) || !addJob(scheduler, jobName, groupName, triggerKey, interval, repeatCount, context)) {
            return false;
        }
        return true;
    }

    private boolean isTriggerKeyExist(Scheduler scheduler, String jobName, String groupName, TriggerKey triggerKey) {
        try {
            if (scheduler.checkExists(triggerKey)) {
                LOGGER.error("The triggerKey({},{})  has been used.", jobName, groupName);
                return true;
            }
        } catch (SchedulerException e) {
            LOGGER.error("Schedule exception.{}", e.getMessage());
        }
        return false;
    }

    private boolean addJob(Scheduler scheduler, String jobName, String groupName, TriggerKey triggerKey, Long interval, Long repeatCount, JobExecutionContext context) {
        try {
            JobDetail jobDetail = addJobDetail(scheduler, jobName, groupName, context);
            scheduler.scheduleJob(newTriggerInstance(triggerKey, jobDetail, interval, repeatCount));
            return true;
        } catch (Exception e) {
            LOGGER.error("Add job failure.{}", e.getMessage());
        }
        return false;
    }


    private Trigger newTriggerInstance(TriggerKey triggerKey, JobDetail jobDetail, Long interval, Long repeatCount) throws ParseException {
        return newTrigger()
                .withIdentity(triggerKey)
                .forJob(jobDetail)
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInMilliseconds(interval)
                        .withRepeatCount(Math.toIntExact(repeatCount))
                )
                .build();
    }

    private JobDetail addJobDetail(Scheduler scheduler, String jobName, String groupName, JobExecutionContext context) throws SchedulerException, JsonProcessingException {
        JobKey jobKey = jobKey(jobName, groupName);
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
