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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.error.exception.GriffinException.GetHealthInfoFailureException;
import org.apache.griffin.core.error.exception.GriffinException.GetJobsFailureException;
import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.JobRequestBody;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.apache.griffin.core.util.GriffinUtil;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.apache.griffin.core.util.GriffinOperationMessage.*;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

@Service
public class JobServiceImpl implements JobService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceImpl.class);

    @Autowired
    private SchedulerFactoryBean factory;
    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    private Properties sparkJobProps;

    public JobServiceImpl() {
    }

    @Override
    public List<Map<String, Serializable>> getAliveJobs() {
        Scheduler scheduler = factory.getObject();
        List<Map<String, Serializable>> list = new ArrayList<>();
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                    Map jobInfoMap = getJobInfoMap(scheduler, jobKey);
                    if (jobInfoMap.size() != 0 && !isJobDeleted(scheduler, jobKey)) {
                        list.add(jobInfoMap);
                    }
                }
            }
        } catch (SchedulerException e) {
            LOGGER.error("failed to get running jobs.{}", e.getMessage());
            throw new GetJobsFailureException();
        }
        return list;
    }

    private boolean isJobDeleted(Scheduler scheduler, JobKey jobKey) throws SchedulerException {
        JobDataMap jobDataMap = scheduler.getJobDetail(jobKey).getJobDataMap();
        return jobDataMap.getBooleanFromString("deleted");
    }

    private Map getJobInfoMap(Scheduler scheduler, JobKey jobKey) throws SchedulerException {
        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
        Map<String, Serializable> jobInfoMap = new HashMap<>();
        if (triggers == null || triggers.size() == 0) {
            return jobInfoMap;
        }
        JobDetail jd = scheduler.getJobDetail(jobKey);
        Date nextFireTime = triggers.get(0).getNextFireTime();
        Date previousFireTime = triggers.get(0).getPreviousFireTime();
        Trigger.TriggerState triggerState = scheduler.getTriggerState(triggers.get(0).getKey());

        jobInfoMap.put("jobName", jobKey.getName());
        jobInfoMap.put("groupName", jobKey.getGroup());
        if (nextFireTime != null) {
            jobInfoMap.put("nextFireTime", nextFireTime.getTime());
        } else {
            jobInfoMap.put("nextFireTime", -1);
        }
        if (previousFireTime != null) {
            jobInfoMap.put("previousFireTime", previousFireTime.getTime());
        } else {
            jobInfoMap.put("previousFireTime", -1);
        }
        jobInfoMap.put("triggerState", triggerState);
        jobInfoMap.put("measureId", jd.getJobDataMap().getString("measureId"));
        jobInfoMap.put("sourcePattern", jd.getJobDataMap().getString("sourcePattern"));
        jobInfoMap.put("targetPattern", jd.getJobDataMap().getString("targetPattern"));
        if (StringUtils.isNotEmpty(jd.getJobDataMap().getString("blockStartTimestamp"))) {
            jobInfoMap.put("blockStartTimestamp", jd.getJobDataMap().getString("blockStartTimestamp"));
        }
        jobInfoMap.put("jobStartTime", jd.getJobDataMap().getString("jobStartTime"));
        jobInfoMap.put("interval", jd.getJobDataMap().getString("interval"));
        return jobInfoMap;
    }

    @Override
    public GriffinOperationMessage addJob(String groupName, String jobName, Long measureId, JobRequestBody jobRequestBody) {
        int interval;
        Date jobStartTime;
        try {
            interval = Integer.parseInt(jobRequestBody.getInterval());
            jobStartTime = new Date(Long.parseLong(jobRequestBody.getJobStartTime()));
            setJobStartTime(jobStartTime, interval);
        } catch (Exception e) {
            LOGGER.info("jobStartTime or interval format error! {}", e.getMessage());
            return CREATE_JOB_FAIL;
        }
        try {
            Scheduler scheduler = factory.getObject();
            TriggerKey triggerKey = triggerKey(jobName, groupName);
            if (scheduler.checkExists(triggerKey)) {
                LOGGER.error("the triggerKey(jobName,groupName) {} has been used.", jobName);
                return CREATE_JOB_FAIL;
            }
            JobKey jobKey = jobKey(jobName, groupName);
            JobDetail jobDetail;
            if (scheduler.checkExists(jobKey)) {
                jobDetail = scheduler.getJobDetail(jobKey);
                setJobData(jobDetail, jobRequestBody, measureId, groupName, jobName);
                scheduler.addJob(jobDetail, true);
            } else {
                jobDetail = newJob(SparkSubmitJob.class)
                        .storeDurably()
                        .withIdentity(jobKey)
                        .build();
                //set JobData
                setJobData(jobDetail, jobRequestBody, measureId, groupName, jobName);
                scheduler.addJob(jobDetail, false);
            }
            Trigger trigger = newTrigger()
                    .withIdentity(triggerKey)
                    .forJob(jobDetail)
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInSeconds(interval)
                            .repeatForever())
                    .startAt(jobStartTime)
                    .build();
            scheduler.scheduleJob(trigger);
            return GriffinOperationMessage.CREATE_JOB_SUCCESS;
        } catch (SchedulerException e) {
            LOGGER.error("SchedulerException when add job. {}", e.getMessage());
            return CREATE_JOB_FAIL;
        }
    }

    private void setJobStartTime(Date jobStartTime, int interval) {
        long currentTimestamp = System.currentTimeMillis();
        long jobStartTimestamp = jobStartTime.getTime();
        //if jobStartTime is before currentTimestamp, reset it with a future time
        if (jobStartTime.before(new Date(currentTimestamp))) {
            long n = (currentTimestamp - jobStartTimestamp) / (long) (interval * 1000);
            jobStartTimestamp = jobStartTimestamp + (n + 1) * (long) (interval * 1000);
            jobStartTime.setTime(jobStartTimestamp);
        }
    }

    private void setJobData(JobDetail jobDetail, JobRequestBody jobRequestBody, Long measureId, String groupName, String jobName) {
        jobDetail.getJobDataMap().put("groupName", groupName);
        jobDetail.getJobDataMap().put("jobName", jobName);
        jobDetail.getJobDataMap().put("measureId", measureId.toString());
        jobDetail.getJobDataMap().put("sourcePattern", jobRequestBody.getSourcePattern());
        jobDetail.getJobDataMap().put("targetPattern", jobRequestBody.getTargetPattern());
        jobDetail.getJobDataMap().put("blockStartTimestamp", jobRequestBody.getBlockStartTimestamp());
        jobDetail.getJobDataMap().put("jobStartTime", jobRequestBody.getJobStartTime());
        jobDetail.getJobDataMap().put("interval", jobRequestBody.getInterval());
        jobDetail.getJobDataMap().put("lastBlockStartTimestamp", "");
        jobDetail.getJobDataMap().putAsString("deleted", false);
    }

    @Override
    public GriffinOperationMessage pauseJob(String group, String name) {
        try {
            Scheduler scheduler = factory.getObject();
            scheduler.pauseJob(new JobKey(name, group));
            return GriffinOperationMessage.PAUSE_JOB_SUCCESS;
        } catch (SchedulerException | NullPointerException e) {
            LOGGER.error("{} {}", GriffinOperationMessage.PAUSE_JOB_FAIL, e.getMessage());
            return GriffinOperationMessage.PAUSE_JOB_FAIL;
        }
    }

    private GriffinOperationMessage setJobDeleted(String group, String name) {
        try {
            Scheduler scheduler = factory.getObject();
            JobDetail jobDetail = scheduler.getJobDetail(new JobKey(name, group));
            jobDetail.getJobDataMap().putAsString("deleted", true);
            scheduler.addJob(jobDetail, true);
            return GriffinOperationMessage.SET_JOB_DELETED_STATUS_SUCCESS;
        } catch (SchedulerException | NullPointerException e) {
            LOGGER.error("{} {}", GriffinOperationMessage.PAUSE_JOB_FAIL, e.getMessage());
            return GriffinOperationMessage.SET_JOB_DELETED_STATUS_FAIL;
        }
    }

    /**
     * logically delete
     * 1. pause these jobs
     * 2. set these jobs as deleted status
     *
     * @param group
     * @param name
     * @return
     */
    @Override
    public GriffinOperationMessage deleteJob(String group, String name) {
        //logically delete
        if (pauseJob(group, name).equals(PAUSE_JOB_SUCCESS) &&
                setJobDeleted(group, name).equals(SET_JOB_DELETED_STATUS_SUCCESS)) {
            return GriffinOperationMessage.DELETE_JOB_SUCCESS;
        }
        return GriffinOperationMessage.DELETE_JOB_FAIL;
    }

    /**
     * deleteJobsRelateToMeasure
     * 1. search jobs related to measure
     * 2. deleteJob
     *
     * @param measure
     */
    //TODO
    public void deleteJobsRelateToMeasure(Measure measure) throws SchedulerException {
        Scheduler scheduler = factory.getObject();
        for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.anyGroup())) {//get all jobs
            JobDetail jobDetail = scheduler.getJobDetail(jobKey);
            JobDataMap jobDataMap = jobDetail.getJobDataMap();
            if (jobDataMap.getString("measureId").equals(measure.getId().toString())) {
                //select jobs related to measureId,
                deleteJob(jobKey.getGroup(), jobKey.getName());
                LOGGER.info("{} {} is paused and logically deleted.", jobKey.getGroup(), jobKey.getName());
            }
        }
    }

    @Override
    public List<JobInstance> findInstancesOfJob(String group, String jobName, int page, int size) {
        //query and return instances
        Pageable pageRequest = new PageRequest(page, size, Sort.Direction.DESC, "timestamp");
        return jobInstanceRepo.findByGroupNameAndJobName(group, jobName, pageRequest);
    }

    @Scheduled(fixedDelayString = "${jobInstance.fixedDelay.in.milliseconds}")
    public void syncInstancesOfAllJobs() {
        List<Object> groupJobList = jobInstanceRepo.findGroupWithJobName();
        for (Object groupJobObj : groupJobList) {
            try {
                Object[] groupJob = (Object[]) groupJobObj;
                if (groupJob != null && groupJob.length == 2) {
                    syncInstancesOfJob(groupJob[0].toString(), groupJob[1].toString());
                }
            } catch (Exception e) {
                LOGGER.error("schedule update instances of all jobs failed. {}", e.getMessage());
            }
        }
    }

    /**
     * call livy to update jobInstance table in mysql.
     *
     * @param group
     * @param jobName
     */
    private void syncInstancesOfJob(String group, String jobName) {
        //update all instance info belongs to this group and job.
        List<JobInstance> jobInstanceList = jobInstanceRepo.findByGroupNameAndJobName(group, jobName);
        for (JobInstance jobInstance : jobInstanceList) {
            if (!LivySessionStates.isActive(jobInstance.getState())) {
                continue;
            }
            String uri = sparkJobProps.getProperty("livy.uri") + "/" + jobInstance.getSessionId();
            RestTemplate restTemplate = new RestTemplate();
            String resultStr;
            try {
                resultStr = restTemplate.getForObject(uri, String.class);
            } catch (Exception e) {
                LOGGER.error("spark session {} has overdue, set state as unknown!\n {}", jobInstance.getSessionId(), e.getMessage());
                //if server cannot get session from Livy, set State as unknown.
                jobInstance.setState(LivySessionStates.State.unknown);
                jobInstanceRepo.save(jobInstance);
                continue;
            }
            TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {
            };
            HashMap<String, Object> resultMap;
            try {
                resultMap = GriffinUtil.toEntity(resultStr, type);
            } catch (IOException e) {
                LOGGER.error("jobInstance jsonStr convert to map failed. {}", e.getMessage());
                continue;
            }
            try {
                if (resultMap != null && resultMap.size() != 0) {
                    jobInstance.setState(LivySessionStates.State.valueOf(resultMap.get("state").toString()));
                    jobInstance.setAppId(resultMap.get("appId").toString());
                    jobInstance.setAppUri(sparkJobProps.getProperty("spark.uri") + "/cluster/app/" + resultMap.get("appId").toString());
                }
            } catch (Exception e) {
                LOGGER.warn("{},{} job Instance has some null field (state or appId). {}", group, jobName, e.getMessage());
                continue;
            }
            jobInstanceRepo.save(jobInstance);
        }
    }

    /**
     * a job is regard as healthy job when its latest instance is in healthy state.
     *
     * @return
     */
    @Override
    public JobHealth getHealthInfo() {
        Scheduler scheduler = factory.getObject();
        int jobCount = 0;
        int notHealthyCount = 0;
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                    jobCount++;
                    String jobName = jobKey.getName();
                    String jobGroup = jobKey.getGroup();
                    Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "timestamp");
                    JobInstance latestJobInstance;
                    if (jobInstanceRepo.findByGroupNameAndJobName(jobGroup, jobName, pageRequest) != null
                            && jobInstanceRepo.findByGroupNameAndJobName(jobGroup, jobName, pageRequest).size() > 0) {
                        latestJobInstance = jobInstanceRepo.findByGroupNameAndJobName(jobGroup, jobName, pageRequest).get(0);
                        if (!LivySessionStates.isHeathy(latestJobInstance.getState())) {
                            notHealthyCount++;
                        }
                    }
                }
            }
        } catch (SchedulerException e) {
            LOGGER.error(e.getMessage());
            throw new GetHealthInfoFailureException();
        }
        return new JobHealth(jobCount - notHealthyCount, jobCount);
    }
}
