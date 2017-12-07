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
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.job.repo.JobScheduleRepo;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.apache.griffin.core.util.JsonUtil;
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
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
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
    private Properties livyConfProps;
    @Autowired
    private MeasureRepo measureRepo;
    @Autowired
    private JobScheduleRepo jobScheduleRepo;

    private RestTemplate restTemplate;


    public JobServiceImpl() {
        restTemplate = new RestTemplate();
    }

    @Override
    public List<Map<String, Serializable>> getAliveJobs() {
        Scheduler scheduler = factory.getObject();
        List<Map<String, Serializable>> list = new ArrayList<>();
        try {
            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.anyGroup())) {
                Map jobInfoMap = getJobInfoMap(scheduler, jobKey);
                if (jobInfoMap.size() != 0 && !isJobDeleted(scheduler, jobKey)) {
                    list.add(jobInfoMap);
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
        if (CollectionUtils.isEmpty(triggers)) {
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
    public GriffinOperationMessage addJob(JobSchedule jobSchedule) {
        Scheduler scheduler = factory.getObject();
        Measure measure = isMeasureIdValid(jobSchedule.getMeasureId());
        if (measure != null) {
            List<String> indexes = getConnectorIndexes(measure);
            if (!isParamValid(jobSchedule.getBaseline(), indexes) || !isConnectorIndexesValid(jobSchedule.getSegments(), indexes)) {
                return CREATE_JOB_FAIL;
            }
            String groupName = "BA";
            String jobName = measure.getName() + "_" + groupName + "_" + System.currentTimeMillis();
            TriggerKey triggerKey = triggerKey(jobName, groupName);
            try {
                if (!scheduler.checkExists(triggerKey) && saveAndAddJob(scheduler, triggerKey, jobSchedule)) {
                    return CREATE_JOB_SUCCESS;
                }
            } catch (Exception e) {
                LOGGER.error("Add job exception happens.", e);
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            }
        }
        return CREATE_JOB_FAIL;
    }

    private boolean isConnectorIndexesValid(List<JobDataSegment> segments, List<String> indexes) {
        for (JobDataSegment segment : segments) {
            if (isParamValid(segment.getDataConnectorIndex(), indexes)) {
                return true;
            }
        }
        return false;
    }

    private boolean isParamValid(String param, List<String> indexes) {
        for (String index : indexes) {
            if (index.equals(param)) {
                return true;
            }
        }
        LOGGER.error("Param {} is a illegal string.Please input one of strings in {}", param, indexes);
        return false;
    }

    private List<String> getConnectorIndexes(Measure measure) {
        List<String> index = new ArrayList<>();
        List<DataSource> sources = measure.getDataSources();
        for (DataSource source : sources) {
            int length = source.getConnectors().size();
            for (int i = 0; i < length; i++) {
                index.add(getConnectorIndex(source, i));
            }
        }
        return index;
    }

    private String getConnectorIndex(DataSource source, int index) {
        StringBuilder sb = new StringBuilder();
        sb.append(source.getName());
        sb.append("[").append(index).append("]");
        return sb.toString();
    }

    private Measure isMeasureIdValid(long measureId) {
        Measure measure = measureRepo.findOne(measureId);
        if (measure != null && !measure.getDeleted()) {
            return measure;
        }
        LOGGER.error("The measure id {} isn't valid. Maybe it doesn't exist or is deleted.", measureId);
        return null;
    }


    private boolean saveAndAddJob(Scheduler scheduler, TriggerKey triggerKey, JobSchedule jobSchedule) throws SchedulerException, ParseException {
        jobSchedule = jobScheduleRepo.save(jobSchedule);
        JobDetail jobDetail = addJobDetail(scheduler, triggerKey, jobSchedule);
        scheduler.scheduleJob(newTriggerInstance(triggerKey, jobDetail, jobSchedule));
        return true;
    }


    private Trigger newTriggerInstance(TriggerKey triggerKey, JobDetail jobDetail, JobSchedule jobSchedule) throws ParseException {
        return newTrigger()
                .withIdentity(triggerKey)
                .forJob(jobDetail)
                .withSchedule(CronScheduleBuilder.cronSchedule(new CronExpression(jobSchedule.getCronExpression()))
                        .inTimeZone(TimeZone.getTimeZone(jobSchedule.getTimeZone()))
                )
                .build();
    }

    private JobDetail addJobDetail(Scheduler scheduler, TriggerKey triggerKey, JobSchedule jobSchedule) throws SchedulerException {
        JobKey jobKey = jobKey(triggerKey.getName(), triggerKey.getGroup());
        JobDetail jobDetail;
        Boolean isJobKeyExist = scheduler.checkExists(jobKey);
        if (isJobKeyExist) {
            jobDetail = scheduler.getJobDetail(jobKey);
        } else {
            jobDetail = newJob(JobInstance.class).storeDurably().withIdentity(jobKey).build();
        }
        setJobDataMap(jobDetail, jobSchedule);
        scheduler.addJob(jobDetail, isJobKeyExist);
        return jobDetail;
    }


    private void setJobDataMap(JobDetail jobDetail, JobSchedule jobSchedule) {
        jobDetail.getJobDataMap().put("measureId", jobSchedule.getMeasureId().toString());
        jobDetail.getJobDataMap().put("jobScheduleId", jobSchedule.getId().toString());
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
     * @param group job group name
     * @param name  job name
     * @return custom information
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
     * @param measure measure data quality between source and target dataset
     * @throws SchedulerException quartz throws if schedule has problem
     */
    public void deleteJobsRelateToMeasure(Measure measure) throws SchedulerException {
        Scheduler scheduler = factory.getObject();
        //get all jobs
        for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.anyGroup())) {
            JobDetail jobDetail = scheduler.getJobDetail(jobKey);
            JobDataMap jobDataMap = jobDetail.getJobDataMap();
            String measureId = jobDataMap.getString("measureId");
            if (measureId != null && measureId.equals(measure.getId().toString())) {
                //select jobs related to measureId
                deleteJob(jobKey.getGroup(), jobKey.getName());
                LOGGER.info("{} {} is paused and logically deleted.", jobKey.getGroup(), jobKey.getName());
            }
        }
    }

    @Override
    public List<JobInstanceBean> findInstancesOfJob(String group, String jobName, int page, int size) {
        try {
            Scheduler scheduler = factory.getObject();
            JobKey jobKey = new JobKey(jobName, group);
            if (!scheduler.checkExists(jobKey) || isJobDeleted(scheduler, jobKey)) {
                return new ArrayList<>();
            }
        } catch (SchedulerException e) {
            LOGGER.error("Quartz schedule error. {}", e.getMessage());
            return new ArrayList<>();
        }
        //query and return instances
        Pageable pageRequest = new PageRequest(page, size, Sort.Direction.DESC, "timestamp");
        return jobInstanceRepo.findByGroupNameAndJobName(group, jobName, pageRequest);
    }

    @Scheduled(fixedDelayString = "${jobInstance.fixedDelay.in.milliseconds}")
    public void syncInstancesOfAllJobs() {
        List<Object> groupJobList = jobInstanceRepo.findGroupAndJobNameWithState();
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
     * call livy to update part of jobInstance table data associated with group and jobName in mysql.
     *
     * @param group   group name of jobInstance
     * @param jobName job name of jobInstance
     */
    private void syncInstancesOfJob(String group, String jobName) {
        //update all instance info belongs to this group and job.
        List<JobInstanceBean> jobInstanceList = jobInstanceRepo.findByGroupNameAndJobName(group, jobName);
        for (JobInstanceBean jobInstance : jobInstanceList) {
            if (LivySessionStates.isActive(jobInstance.getState())) {
                String uri = livyConfProps.getProperty("livy.uri") + "/" + jobInstance.getSessionId();
                setJobInstanceInfo(jobInstance, uri, group, jobName);
            }

        }
    }

    private void setJobInstanceInfo(JobInstanceBean jobInstance, String uri, String group, String jobName) {
        TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {
        };
        try {
            String resultStr = restTemplate.getForObject(uri, String.class);
            HashMap<String, Object> resultMap = JsonUtil.toEntity(resultStr, type);
            setJobInstanceIdAndUri(jobInstance, resultMap);
        } catch (RestClientException e) {
            LOGGER.error("spark session {} has overdue, set state as unknown!\n {}", jobInstance.getSessionId(), e.getMessage());
            setJobInstanceUnknownStatus(jobInstance);
        } catch (IOException e) {
            LOGGER.error("jobInstance jsonStr convert to map failed. {}", e.getMessage());
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Livy status is illegal. {}", group, jobName, e.getMessage());
        }
    }

    private void setJobInstanceIdAndUri(JobInstanceBean jobInstance, HashMap<String, Object> resultMap) throws IllegalArgumentException {
        if (resultMap != null && resultMap.size() != 0 && resultMap.get("state") != null) {
            jobInstance.setState(LivySessionStates.State.valueOf(resultMap.get("state").toString()));
            if (resultMap.get("appId") != null) {
                jobInstance.setAppId(resultMap.get("appId").toString());
                jobInstance.setAppUri(livyConfProps.getProperty("spark.uri") + "/cluster/app/" + resultMap.get("appId").toString());
            }
            jobInstanceRepo.save(jobInstance);
        }

    }

    private void setJobInstanceUnknownStatus(JobInstanceBean jobInstance) {
        //if server cannot get session from Livy, set State as unknown.
        jobInstance.setState(LivySessionStates.State.unknown);
        jobInstanceRepo.save(jobInstance);
    }

    /**
     * a job is regard as healthy job when its latest instance is in healthy state.
     *
     * @return job healthy statistics
     */
    @Override
    public JobHealth getHealthInfo() {
        Scheduler scheduler = factory.getObject();
        int jobCount = 0;
        int notHealthyCount = 0;
        try {
            Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.anyGroup());
            for (JobKey jobKey : jobKeys) {
                List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
                if (triggers != null && triggers.size() != 0 && !isJobDeleted(scheduler, jobKey)) {
                    jobCount++;
                    notHealthyCount = getJobNotHealthyCount(notHealthyCount, jobKey);
                }
            }
        } catch (SchedulerException e) {
            LOGGER.error(e.getMessage());
            throw new GetHealthInfoFailureException();
        }
        return new JobHealth(jobCount - notHealthyCount, jobCount);
    }

    private int getJobNotHealthyCount(int notHealthyCount, JobKey jobKey) {
        if (!isJobHealthy(jobKey)) {
            notHealthyCount++;
        }
        return notHealthyCount;
    }

    private Boolean isJobHealthy(JobKey jobKey) {
        Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "timestamp");
        JobInstanceBean latestJobInstance;
        List<JobInstanceBean> jobInstances = jobInstanceRepo.findByGroupNameAndJobName(jobKey.getGroup(), jobKey.getName(), pageRequest);
        if (jobInstances != null && jobInstances.size() > 0) {
            latestJobInstance = jobInstances.get(0);
            if (LivySessionStates.isHealthy(latestJobInstance.getState())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Map<String, List<Map<String, Serializable>>> getJobDetailsGroupByMeasureId() {
        Map<String, List<Map<String, Serializable>>> jobDetailsMap = new HashMap<>();
        List<Map<String, Serializable>> jobInfoList = getAliveJobs();
        for (Map<String, Serializable> jobInfo : jobInfoList) {
            String measureId = (String) jobInfo.get("measureId");
            List<Map<String, Serializable>> jobs = jobDetailsMap.getOrDefault(measureId, new ArrayList<>());
            jobs.add(jobInfo);
            jobDetailsMap.put(measureId, jobs);
        }
        return jobDetailsMap;
    }
}
