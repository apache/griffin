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
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.job.repo.GriffinJobRepo;
import org.apache.griffin.core.job.repo.JobScheduleRepo;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.repo.GriffinMeasureRepo;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.griffin.core.util.TimeUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.io.IOException;
import java.util.*;

import static org.apache.griffin.core.job.JobServiceImpl.GRIFFIN_JOB_ID;
import static org.apache.griffin.core.job.JobServiceImpl.JOB_SCHEDULE_ID;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class JobInstance implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobInstance.class);
    public static final String MEASURE_KEY = "measure";
    public static final String PREDICATES_KEY = "predicts";
    public static final String PREDICATE_JOB_NAME = "predicateJobName";
    static final String JOB_NAME = "jobName";
    static final String PATH_CONNECTOR_CHARACTER = ",";

    @Autowired
    private SchedulerFactoryBean factory;
    @Autowired
    private GriffinMeasureRepo measureRepo;
    @Autowired
    private GriffinJobRepo jobRepo;
    @Autowired
    private JobScheduleRepo jobScheduleRepo;
    @Autowired
    @Qualifier("appConf")
    private Properties appConfProps;

    private JobSchedule jobSchedule;
    private GriffinMeasure measure;
    private GriffinJob griffinJob;
    private List<SegmentPredicate> mPredicates;
    private Long jobStartTime;


    @Override
    public void execute(JobExecutionContext context) {
        try {
            initParam(context);
            setSourcesPartitionsAndPredicates(measure.getDataSources());
            createJobInstance(jobSchedule.getConfigMap());
        } catch (Exception e) {
            LOGGER.error("Create predicate job failure.", e);
        }
    }

    private void initParam(JobExecutionContext context) throws SchedulerException {
        mPredicates = new ArrayList<>();
        JobDetail jobDetail = context.getJobDetail();
        Long jobScheduleId = jobDetail.getJobDataMap().getLong(JOB_SCHEDULE_ID);
        Long griffinJobId = jobDetail.getJobDataMap().getLong(GRIFFIN_JOB_ID);
        jobSchedule = jobScheduleRepo.findOne(jobScheduleId);
        Long measureId = jobSchedule.getMeasureId();
        griffinJob = jobRepo.findOne(griffinJobId);
        measure = measureRepo.findOne(measureId);
        setJobStartTime(jobDetail);

    }

    private void setJobStartTime(JobDetail jobDetail) throws SchedulerException {
        Scheduler scheduler = factory.getObject();
        JobKey jobKey = jobDetail.getKey();
        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
        Date triggerTime = triggers.get(0).getPreviousFireTime();
        jobStartTime = triggerTime.getTime();
    }


    private void setSourcesPartitionsAndPredicates(List<DataSource> sources) throws Exception {
        boolean isFirstBaseline = true;
        for (JobDataSegment jds : jobSchedule.getSegments()) {
            if (jds.getBaseline() && isFirstBaseline) {
                Long tsOffset = TimeUtil.str2Long(jds.getSegmentRange().getBegin());
                measure.setTimestamp(jobStartTime + tsOffset);
                isFirstBaseline = false;
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
    private Long[] genSampleTs(SegmentRange segRange, DataConnector dc) {
        Long offset = TimeUtil.str2Long(segRange.getBegin());
        Long range = TimeUtil.str2Long(segRange.getLength());
        String unit = dc.getDataUnit();
        Long dataUnit = TimeUtil.str2Long(StringUtils.isEmpty(unit) ? dc.getDefaultDataUnit() : unit);
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
        for (SegmentPredicate predicate : predicates) {
            genConfMap(dc, sampleTs);
            //Do not forget to update origin string config
            predicate.setConfigMap(predicate.getConfigMap());
            mPredicates.add(predicate);
        }
    }

    /**
     * set data connector configs
     *
     * @param sampleTs collection of data split start timestamp
     */
    private void setConnectorConf(DataConnector dc, Long[] sampleTs) throws IOException {
        genConfMap(dc, sampleTs);
        dc.setConfigMap(dc.getConfigMap());
    }


    /**
     * @param dc     data connector
     * @param sampleTs collection of data split start timestamp
     * @return all config data combine,like {"where": "year=2017 AND month=11 AND dt=15 AND hour=09,year=2017 AND month=11 AND dt=15 AND hour=10"}
     * or like {"path": "/year=2017/month=11/dt=15/hour=09/_DONE,/year=2017/month=11/dt=15/hour=10/_DONE"}
     */
    private void genConfMap(DataConnector dc,  Long[] sampleTs) {
        Map<String, String> conf = dc.getConfigMap();
        if (conf == null) {
            LOGGER.warn("Predicate config is null.");
            return;
        }
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            String value = entry.getValue();
            Set<String> set = new HashSet<>();
            if (StringUtils.isEmpty(value)) {
                continue;
            }
            for (Long timestamp : sampleTs) {
                set.add(TimeUtil.format(value, timestamp, getTimeZone(dc)));
            }
            conf.put(entry.getKey(), StringUtils.join(set, PATH_CONNECTOR_CHARACTER));
        }
    }

    private TimeZone getTimeZone(DataConnector dc) {
        if (StringUtils.isEmpty(dc.getDataTimeZone())) {
            return TimeZone.getDefault();
        }
        return TimeZone.getTimeZone(dc.getDataTimeZone());
    }

    private boolean createJobInstance(Map<String, Object> confMap) throws Exception {
        Map<String, Object> config = (Map<String, Object>) confMap.get("checkdonefile.schedule");
        Long interval = TimeUtil.str2Long((String) config.get("interval"));
        Integer repeat = Integer.valueOf(config.get("repeat").toString());
        String groupName = "PG";
        String jobName = griffinJob.getJobName() + "_predicate_" + System.currentTimeMillis();
        Scheduler scheduler = factory.getObject();
        TriggerKey triggerKey = triggerKey(jobName, groupName);
        return !(scheduler.checkExists(triggerKey)
                || !saveGriffinJob(jobName, groupName)
                || !createJobInstance(triggerKey, interval, repeat, jobName));
    }

    private boolean saveGriffinJob(String pName, String pGroup) {
        List<JobInstanceBean> instances = griffinJob.getJobInstances();
        Long tms = System.currentTimeMillis();
        Long expireTms = Long.valueOf(appConfProps.getProperty("jobInstance.expired.milliseconds")) + tms;
        instances.add(new JobInstanceBean(LivySessionStates.State.finding, pName, pGroup, tms, expireTms));
        griffinJob = jobRepo.save(griffinJob);
        return true;
    }

    private boolean createJobInstance(TriggerKey triggerKey, Long interval, Integer repeatCount, String pJobName) throws Exception {
        JobDetail jobDetail = addJobDetail(triggerKey, pJobName);
        factory.getObject().scheduleJob(newTriggerInstance(triggerKey, jobDetail, interval, repeatCount));
        return true;
    }


    private Trigger newTriggerInstance(TriggerKey triggerKey, JobDetail jd, Long interval, Integer repeatCount) {
        return newTrigger()
                .withIdentity(triggerKey)
                .forJob(jd)
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInMilliseconds(interval)
                        .withRepeatCount(repeatCount)
                )
                .build();
    }

    private JobDetail addJobDetail(TriggerKey triggerKey, String pJobName) throws SchedulerException, JsonProcessingException {
        Scheduler scheduler = factory.getObject();
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
        setJobDataMap(jobDetail, pJobName);
        scheduler.addJob(jobDetail, isJobKeyExist);
        return jobDetail;
    }

    private void setJobDataMap(JobDetail jobDetail, String pJobName) throws JsonProcessingException {
        JobDataMap dataMap = jobDetail.getJobDataMap();
        dataMap.put(MEASURE_KEY, JsonUtil.toJson(measure));
        dataMap.put(PREDICATES_KEY, JsonUtil.toJson(mPredicates));
        dataMap.put(JOB_NAME, griffinJob.getJobName());
        dataMap.put(PREDICATE_JOB_NAME, pJobName);
    }

}
