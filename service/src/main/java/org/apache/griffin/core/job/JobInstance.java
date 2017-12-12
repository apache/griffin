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
import org.apache.griffin.core.job.entity.SegmentRange;
import org.apache.griffin.core.job.repo.JobScheduleRepo;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
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
    private MeasureRepo<GriffinMeasure> measureRepo;
    @Autowired
    private JobScheduleRepo jobScheduleRepo;

    private JobSchedule jobSchedule;
    private GriffinMeasure measure;
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
        for (JobDataSegment jds : jobSchedule.getSegments()) {
            if (jds.getBaseline()) {
                Long tsOffset = TimeUtil.str2Long(jds.getSegmentRange().getBegin());
                measure.setTimestamp(jobStartTime + tsOffset);
            }
            for (DataSource ds : sources) {
                setDataSourcePartitions(jds, ds);
            }
        }
    }

    private void setDataSourcePartitions(JobDataSegment jds, DataSource ds) throws Exception {
        List<DataConnector> connectors = ds.getConnectors();
        for (DataConnector dc : connectors) {
            setDataConnectorPartitions(jds, dc);
        }
    }


    private void setDataConnectorPartitions(JobDataSegment jds, DataConnector dc) throws Exception {
        String dcName = jds.getDataConnectorName();
        if (dcName.equals(dc.getName())) {
            Long[] sampleTs = genSampleTs(jds.getSegmentRange(), dc);
            setConnectorConf(dc, sampleTs);
            setConnectorPredicates(dc, sampleTs);
        }
    }

    /**
     * split data into several part and get every part start timestamp
     *
     * @param segRange config of data
     * @return split timestamps of data
     */
    private Long[] genSampleTs(SegmentRange segRange, DataConnector dc) throws IOException {
        Long offset = TimeUtil.str2Long(segRange.getBegin());
        Long range = TimeUtil.str2Long(segRange.getLength());
        Long dataUnit = TimeUtil.str2Long(dc.getDataUnit());
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
     * set data connector predicates
     *
     * @param sampleTs collection of data split start timestamp
     */
    private void setConnectorPredicates(DataConnector dc, Long[] sampleTs) throws IOException {
        List<SegmentPredicate> predicates = dc.getPredicates();
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
     * set data connector configs
     *
     * @param sampleTs collection of data split start timestamp
     */
    private void setConnectorConf(DataConnector dc, Long[] sampleTs) throws IOException {
        genConfMap(dc.getConfigMap(), sampleTs);
        dc.setConfigMap(dc.getConfigMap());
    }


    /**
     * @param conf     map with file predicate,data split and partitions info
     * @param sampleTs collection of data split start timestamp
     * @return all config data combine,like {"where": "year=2017 AND month=11 AND dt=15 AND hour=09,year=2017 AND month=11 AND dt=15 AND hour=10"}
     * or like {"path": "/year=#2017/month=11/dt=15/hour=09/_DONE,/year=#2017/month=11/dt=15/hour=10/_DONE"}
     */
    private void genConfMap(Map<String, String> conf, Long[] sampleTs) {
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            String value = entry.getValue();
            Set<String> set = new HashSet<>();
            for (Long timestamp : sampleTs) {
                set.add(TimeUtil.format(value, timestamp));
            }
            conf.put(entry.getKey(), StringUtils.join(set, ","));
        }
    }

    private boolean createJobInstance(Map<String, Object> confMap, JobExecutionContext context) throws Exception {
        Map<String, Object> scheduleConfig = (Map<String, Object>) confMap.get("checkdonefile.schedule");
        Long interval = TimeUtil.str2Long((String) scheduleConfig.get("interval"));
        Integer repeat = (Integer) scheduleConfig.get("repeat");
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
