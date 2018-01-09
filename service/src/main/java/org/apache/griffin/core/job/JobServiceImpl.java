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
import org.apache.griffin.core.job.repo.GriffinJobRepo;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.job.repo.JobScheduleRepo;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.GriffinMeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.apache.griffin.core.util.JsonUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
    static final String JOB_SCHEDULE_ID = "jobScheduleId";
    static final String GRIFFIN_JOB_ID = "griffinJobId";
    static final int MAX_PAGE_SIZE = 1024;
    static final int DEFAULT_PAGE_SIZE = 10;

    @Autowired
    private SchedulerFactoryBean factory;
    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    @Qualifier("livyConf")
    private Properties livyConf;
    @Autowired
    private GriffinMeasureRepo measureRepo;
    @Autowired
    private GriffinJobRepo jobRepo;
    @Autowired
    private JobScheduleRepo jobScheduleRepo;

    private RestTemplate restTemplate;

    public JobServiceImpl() {
        restTemplate = new RestTemplate();
    }

    @Override
    public List<JobDataBean> getAliveJobs() {
        Scheduler scheduler = factory.getObject();
        List<JobDataBean> dataList = new ArrayList<>();
        try {
            List<GriffinJob> jobs = jobRepo.findByDeleted(false);
            for (GriffinJob job : jobs) {
                JobDataBean jobData = genJobData(scheduler, jobKey(job.getQuartzName(), job.getQuartzGroup()), job);
                if (jobData != null) {
                    dataList.add(jobData);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to get running jobs.", e);
            throw new GetJobsFailureException();
        }
        return dataList;
    }

    private JobDataBean genJobData(Scheduler scheduler, JobKey jobKey, GriffinJob job) throws SchedulerException {
        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
        if (CollectionUtils.isEmpty(triggers)) {
            return null;
        }
        JobDataBean jobData = new JobDataBean();
        Trigger trigger = triggers.get(0);
        setTriggerTime(trigger, jobData);
        jobData.setJobId(job.getId());
        jobData.setJobName(job.getJobName());
        jobData.setMeasureId(job.getMeasureId());
        jobData.setTriggerState(scheduler.getTriggerState(trigger.getKey()));
        jobData.setCronExpression(getCronExpression(triggers));
        return jobData;
    }

    private String getCronExpression(List<Trigger> triggers) {
        for (Trigger trigger : triggers) {
            if (trigger instanceof CronTrigger) {
                return ((CronTrigger) trigger).getCronExpression();
            }
        }
        return null;
    }

    private void setTriggerTime(Trigger trigger, JobDataBean jobBean) throws SchedulerException {
        Date nextFireTime = trigger.getNextFireTime();
        Date previousFireTime = trigger.getPreviousFireTime();
        jobBean.setNextFireTime(nextFireTime != null ? nextFireTime.getTime() : -1);
        jobBean.setPreviousFireTime(previousFireTime != null ? previousFireTime.getTime() : -1);
    }

    @Override
    public GriffinOperationMessage addJob(JobSchedule js) {
        Long measureId = js.getMeasureId();
        GriffinMeasure measure = getMeasureIfValid(measureId);
        if (measure != null) {
            return addJob(js, measure);
        }
        return CREATE_JOB_FAIL;
    }

    private GriffinOperationMessage addJob(JobSchedule js, GriffinMeasure measure) {
        String qName = js.getJobName() + "_" + System.currentTimeMillis();
        String qGroup = getQuartzGroupName();
        try {
            if (addJob(js, measure, qName, qGroup)) {
                return CREATE_JOB_SUCCESS;
            }
        } catch (Exception e) {
            LOGGER.error("Add job exception happens.", e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        }
        return CREATE_JOB_FAIL;
    }

    private boolean addJob(JobSchedule js, GriffinMeasure measure, String qName, String qGroup) throws SchedulerException, ParseException {
        Scheduler scheduler = factory.getObject();
        TriggerKey triggerKey = triggerKey(qName, qGroup);
        if (!isJobScheduleParamValid(js, measure)) {
            return false;
        }
        if (scheduler.checkExists(triggerKey)) {
            return false;
        }
        GriffinJob job = saveGriffinJob(measure.getId(), js.getJobName(), qName, qGroup);
        return job != null && saveAndAddQuartzJob(scheduler, triggerKey, js, job);
    }

    private String getQuartzGroupName() {
        return "BA";
    }

    private boolean isJobScheduleParamValid(JobSchedule js, GriffinMeasure measure) throws SchedulerException {
        if (!isJobNameValid(js.getJobName())) {
            return false;
        }
        if (!isBaseLineValid(js.getSegments())) {
            return false;
        }
        List<String> names = getConnectorNames(measure);
        return isConnectorNamesValid(js.getSegments(), names);
    }

    private boolean isJobNameValid(String jobName) {
        if (StringUtils.isEmpty(jobName)) {
            LOGGER.warn("Job name cannot be empty.");
            return false;
        }
        int size = jobRepo.countByJobNameAndDeleted(jobName, false);
        if (size > 0) {
            LOGGER.warn("Job name already exits.");
            return false;
        }
        return true;
    }

    private boolean isBaseLineValid(List<JobDataSegment> segments) {
        for (JobDataSegment jds : segments) {
            if (jds.getBaseline()) {
                return true;
            }
        }
        LOGGER.warn("Please set segment timestamp baseline in as.baseline field.");
        return false;
    }

    private boolean isConnectorNamesValid(List<JobDataSegment> segments, List<String> names) {
        for (JobDataSegment segment : segments) {
            if (!isConnectorNameValid(segment.getDataConnectorName(), names)) {
                return false;
            }
        }
        return true;
    }

    private boolean isConnectorNameValid(String param, List<String> names) {
        for (String name : names) {
            if (name.equals(param)) {
                return true;
            }
        }
        LOGGER.warn("Param {} is a illegal string. Please input one of strings in {}.", param, names);
        return false;
    }

    private List<String> getConnectorNames(GriffinMeasure measure) {
        List<String> names = new ArrayList<>();
        Set<String> sets = new HashSet<>();
        List<DataSource> sources = measure.getDataSources();
        for (DataSource source : sources) {
            source.getConnectors().forEach(dc -> {
                sets.add(dc.getName());
            });
        }
        names.addAll(sets);
        if (names.size() < sets.size()) {
            LOGGER.error("Connector names cannot be repeated.");
            throw new IllegalArgumentException();
        }
        return names;
    }

    private GriffinMeasure getMeasureIfValid(Long measureId) {
        Measure measure = measureRepo.findByIdAndDeleted(measureId, false);
        if (measure == null) {
            LOGGER.warn("The measure id {} isn't valid. Maybe it doesn't exist or is deleted.", measureId);
            return null;
        }
        return (GriffinMeasure) measure;
    }

    private GriffinJob saveGriffinJob(Long measureId, String jobName, String qName, String qGroup) {
        GriffinJob job = new GriffinJob(measureId, jobName, qName, qGroup, false);
        return jobRepo.save(job);
    }

    private boolean saveAndAddQuartzJob(Scheduler scheduler, TriggerKey triggerKey, JobSchedule js, GriffinJob job) throws SchedulerException, ParseException {
        js = jobScheduleRepo.save(js);
        JobDetail jobDetail = addJobDetail(scheduler, triggerKey, js, job);
        scheduler.scheduleJob(genTriggerInstance(triggerKey, jobDetail, js));
        return true;
    }


    private Trigger genTriggerInstance(TriggerKey triggerKey, JobDetail jd, JobSchedule js) throws ParseException {
        return newTrigger()
                .withIdentity(triggerKey)
                .forJob(jd)
                .withSchedule(CronScheduleBuilder.cronSchedule(new CronExpression(js.getCronExpression()))
                        .inTimeZone(TimeZone.getTimeZone(js.getTimeZone()))
                )
                .build();
    }

    private JobDetail addJobDetail(Scheduler scheduler, TriggerKey triggerKey, JobSchedule js, GriffinJob job) throws SchedulerException {
        JobKey jobKey = jobKey(triggerKey.getName(), triggerKey.getGroup());
        JobDetail jobDetail;
        Boolean isJobKeyExist = scheduler.checkExists(jobKey);
        if (isJobKeyExist) {
            jobDetail = scheduler.getJobDetail(jobKey);
        } else {
            jobDetail = newJob(JobInstance.class).storeDurably().withIdentity(jobKey).build();
        }
        setJobDataMap(jobDetail, js, job);
        scheduler.addJob(jobDetail, isJobKeyExist);
        return jobDetail;
    }


    private void setJobDataMap(JobDetail jd, JobSchedule js, GriffinJob job) {
        jd.getJobDataMap().put(JOB_SCHEDULE_ID, js.getId().toString());
        jd.getJobDataMap().put(GRIFFIN_JOB_ID, job.getId().toString());
    }

    private boolean pauseJob(List<JobInstanceBean> instances) {
        if (CollectionUtils.isEmpty(instances)) {
            return true;
        }
        List<JobInstanceBean> deletedInstances = new ArrayList<>();
        boolean pauseStatus = true;
        for (JobInstanceBean instance : instances) {
            boolean status = pauseJob(instance, deletedInstances);
            pauseStatus = pauseStatus && status;
        }
        jobInstanceRepo.save(deletedInstances);
        return pauseStatus;
    }

    private boolean pauseJob(JobInstanceBean instance, List<JobInstanceBean> deletedInstances) {
        boolean status;
        try {
            status = pauseJob(instance.getPredicateGroup(), instance.getPredicateName());
            if (status) {
                instance.setDeleted(true);
                deletedInstances.add(instance);
            }
        } catch (SchedulerException e) {
            LOGGER.error("Pause predicate job({},{}) failure.", instance.getId(), instance.getPredicateName());
            status = false;
        }
        return status;
    }

    @Override
    public boolean pauseJob(String group, String name) throws SchedulerException {
        Scheduler scheduler = factory.getObject();
        JobKey jobKey = new JobKey(name, group);
        if (!scheduler.checkExists(jobKey)) {
            LOGGER.warn("Job({},{}) does not exist.", jobKey.getGroup(), jobKey.getName());
            return false;
        }
        scheduler.pauseJob(jobKey);
        return true;
    }

    private boolean setJobDeleted(GriffinJob job) throws SchedulerException {
        job.setDeleted(true);
        jobRepo.save(job);
        return true;
    }

    private boolean deletePredicateJob(GriffinJob job) throws SchedulerException {
        boolean pauseStatus = true;
        List<JobInstanceBean> instances = job.getJobInstances();
        for (JobInstanceBean instance : instances) {
            if (!instance.getDeleted()) {
                pauseStatus = pauseStatus && deleteJob(instance.getPredicateGroup(), instance.getPredicateName());
                instance.setDeleted(true);
                if (instance.getState().equals(LivySessionStates.State.finding)) {
                    instance.setState(LivySessionStates.State.not_found);
                }
            }
        }
        return pauseStatus;
    }

    /**
     * logically delete
     * 1. pause these jobs
     * 2. set these jobs as deleted status
     *
     * @param jobId griffin job id
     * @return custom information
     */
    @Override
    public GriffinOperationMessage deleteJob(Long jobId) {
        GriffinJob job = jobRepo.findByIdAndDeleted(jobId, false);
        return deleteJob(job) ? DELETE_JOB_SUCCESS : DELETE_JOB_FAIL;
    }

    /**
     * logically delete
     *
     * @param name griffin job name which may not be unique.
     * @return custom information
     */
    @Override
    public GriffinOperationMessage deleteJob(String name) {
        List<GriffinJob> jobs = jobRepo.findByJobNameAndDeleted(name, false);
        if (CollectionUtils.isEmpty(jobs)) {
            LOGGER.warn("There is no job with '{}' name.", name);
            return DELETE_JOB_FAIL;
        }
        for (GriffinJob job : jobs) {
            if (!deleteJob(job)) {
                return DELETE_JOB_FAIL;
            }
        }
        return DELETE_JOB_SUCCESS;
    }

    private boolean deleteJob(GriffinJob job) {
        if (job == null) {
            LOGGER.warn("Griffin job does not exist.");
            return false;
        }
        try {
            if (pauseJob(job.getQuartzGroup(), job.getQuartzName()) && deletePredicateJob(job) && setJobDeleted(job)) {
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Delete job failure.", e);
        }
        return false;
    }

    private boolean deleteJob(String group, String name) throws SchedulerException {
        Scheduler scheduler = factory.getObject();
        JobKey jobKey = new JobKey(name, group);
        if (scheduler.checkExists(jobKey)) {
            LOGGER.warn("Job({},{}) does not exist.", jobKey.getGroup(), jobKey.getName());
            return false;
        }
        scheduler.deleteJob(jobKey);
        return true;
    }

    /**
     * deleteJobsRelateToMeasure
     * 1. search jobs related to measure
     * 2. deleteJob
     *
     * @param measureId measure id
     */
    public boolean deleteJobsRelateToMeasure(Long measureId) {
        List<GriffinJob> jobs = jobRepo.findByMeasureIdAndDeleted(measureId, false);
        if (CollectionUtils.isEmpty(jobs)) {
            LOGGER.warn("Measure id {} has no related jobs.", measureId);
            return false;
        }
        for (GriffinJob job : jobs) {
            deleteJob(job);
        }
        return true;
    }

    @Override
    public List<JobInstanceBean> findInstancesOfJob(Long jobId, int page, int size) {
        AbstractJob job = jobRepo.findByIdAndDeleted(jobId, false);
        if (job == null) {
            LOGGER.warn("Job id {} does not exist.", jobId);
            return new ArrayList<>();
        }
        size = size > MAX_PAGE_SIZE ? MAX_PAGE_SIZE : size;
        size = size <= 0 ? DEFAULT_PAGE_SIZE : size;
        Pageable pageable = new PageRequest(page, size, Sort.Direction.DESC, "tms");
        return jobInstanceRepo.findByJobId(jobId, pageable);
    }

    @Scheduled(fixedDelayString = "${jobInstance.expired.milliseconds}")
    public void deleteExpiredJobInstance() {
        List<JobInstanceBean> instances = jobInstanceRepo.findByExpireTmsLessThanEqual(System.currentTimeMillis());
        if (!pauseJob(instances)) {
            LOGGER.error("Pause job failure.");
            return;
        }
        jobInstanceRepo.deleteByExpireTimestamp(System.currentTimeMillis());
        LOGGER.info("Delete expired job instances success.");
    }

    @Scheduled(fixedDelayString = "${jobInstance.fixedDelay.in.milliseconds}")
    public void syncInstancesOfAllJobs() {
        List<JobInstanceBean> beans = jobInstanceRepo.findByActiveState();
        if (!CollectionUtils.isEmpty(beans)) {
            for (JobInstanceBean jobInstance : beans) {
                syncInstancesOfJob(jobInstance);
            }
        }
    }


    /**
     * call livy to update part of job instance table data associated with group and jobName in mysql.
     *
     * @param jobInstance job instance livy info
     */
    private void syncInstancesOfJob(JobInstanceBean jobInstance) {
        String uri = livyConf.getProperty("livy.uri") + "/" + jobInstance.getSessionId();
        TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {
        };
        try {
            String resultStr = restTemplate.getForObject(uri, String.class);
            HashMap<String, Object> resultMap = JsonUtil.toEntity(resultStr, type);
            setJobInstanceIdAndUri(jobInstance, resultMap);
        } catch (RestClientException e) {
            LOGGER.error("Spark session {} has overdue, set state as unknown!\n {}", jobInstance.getSessionId(), e.getMessage());
            setJobInstanceUnknownStatus(jobInstance);
        } catch (IOException e) {
            LOGGER.error("Job instance json converts to map failed. {}", e.getMessage());
        } catch (IllegalArgumentException e) {
            LOGGER.error("Livy status is illegal. {}", e.getMessage());
        }
    }


    private void setJobInstanceIdAndUri(JobInstanceBean instance, HashMap<String, Object> resultMap) {
        if (resultMap != null && resultMap.size() != 0 && resultMap.get("state") != null) {
            instance.setState(LivySessionStates.State.valueOf(resultMap.get("state").toString()));
            if (resultMap.get("appId") != null) {
                String appId = String.valueOf(resultMap.get("appId"));
                String appUri = livyConf.getProperty("spark.uri") + "/cluster/app/" + appId;
                instance.setAppId(appId);
                instance.setAppUri(appUri);
            }
            jobInstanceRepo.save(instance);
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
        JobHealth jobHealth = new JobHealth();
        List<GriffinJob> jobs = jobRepo.findByDeleted(false);
        for (GriffinJob job : jobs) {
            jobHealth = getHealthInfo(jobHealth, job);
        }
        return jobHealth;
    }

    private JobHealth getHealthInfo(JobHealth jobHealth, GriffinJob job) {
        List<Trigger> triggers = getTriggers(job);
        if (!CollectionUtils.isEmpty(triggers)) {
            jobHealth.setJobCount(jobHealth.getJobCount() + 1);
            if (isJobHealthy(job.getId())) {
                jobHealth.setHealthyJobCount(jobHealth.getHealthyJobCount() + 1);
            }
        }
        return jobHealth;
    }

    private List<Trigger> getTriggers(GriffinJob job) {
        JobKey jobKey = new JobKey(job.getQuartzName(), job.getQuartzGroup());
        List<Trigger> triggers;
        try {
            triggers = (List<Trigger>) factory.getObject().getTriggersOfJob(jobKey);
        } catch (SchedulerException e) {
            LOGGER.error("Job schedule exception. {}", e.getMessage());
            throw new GetHealthInfoFailureException();
        }
        return triggers;
    }

    private Boolean isJobHealthy(Long jobId) {
        Pageable pageable = new PageRequest(0, 1, Sort.Direction.DESC, "tms");
        List<JobInstanceBean> instances = jobInstanceRepo.findByJobId(jobId,pageable);
        return !CollectionUtils.isEmpty(instances) && LivySessionStates.isHealthy(instances.get(0).getState());
    }


}
