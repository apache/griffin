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
import org.apache.griffin.core.exception.GriffinException;
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.job.repo.BatchJobRepo;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.job.repo.JobRepo;
import org.apache.griffin.core.job.repo.StreamingJobRepo;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType;
import org.apache.griffin.core.measure.repo.GriffinMeasureRepo;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.griffin.core.util.YarnNetUtil;
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
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static java.util.TimeZone.getTimeZone;
import static org.apache.griffin.core.exception.GriffinExceptionMessage.*;
import static org.apache.griffin.core.job.entity.LivySessionStates.State;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.*;
import static org.apache.griffin.core.job.entity.LivySessionStates.isActive;
import static org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType.BATCH;
import static org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType.STREAMING;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

@Service
public class JobServiceImpl implements JobService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceImpl.class);
    public static final String GRIFFIN_JOB_ID = "griffinJobId";
    private static final int MAX_PAGE_SIZE = 1024;
    private static final int DEFAULT_PAGE_SIZE = 10;
    static final String START = "start";
    static final String STOP = "stop";
    private static final String BATCH_TYPE = "batch";
    private static final String STREAMING_TYPE = "streaming";

    @Autowired
    private SchedulerFactoryBean factory;
    @Autowired
    private JobInstanceRepo instanceRepo;
    @Autowired
    @Qualifier("livyConf")
    private Properties livyConf;
    @Autowired
    private GriffinMeasureRepo measureRepo;
    @Autowired
    private BatchJobRepo batchJobRepo;
    @Autowired
    private StreamingJobRepo streamingJobRepo;
    @Autowired
    private JobRepo<AbstractJob> jobRepo;
    @Autowired
    private BatchJobOperatorImpl batchJobOp;
    @Autowired
    private StreamingJobOperatorImpl streamingJobOp;

    private RestTemplate restTemplate;

    public JobServiceImpl() {
        restTemplate = new RestTemplate();
    }

    @Override
    public List<JobDataBean> getAliveJobs(String type) {
        List<? extends AbstractJob> jobs;
        if (BATCH_TYPE.equals(type)) {
            jobs = batchJobRepo.findByDeleted(false);
        } else if (STREAMING_TYPE.equals(type)) {
            jobs = streamingJobRepo.findByDeleted(false);
        } else {
            jobs = jobRepo.findByDeleted(false);
        }
        return getJobDataBeans(jobs);
    }

    private List<JobDataBean> getJobDataBeans(List<? extends AbstractJob> jobs) {
        List<JobDataBean> dataList = new ArrayList<>();
        try {
            for (AbstractJob job : jobs) {
                JobDataBean jobData = genJobDataBean(job);
                if (jobData != null) {
                    dataList.add(jobData);
                }
            }
        } catch (SchedulerException e) {
            LOGGER.error("Failed to get RUNNING jobs.", e);
            throw new GriffinException.ServiceException("Failed to get RUNNING jobs.", e);
        }
        return dataList;
    }

    @Override
    public JobSchedule getJobSchedule(String jobName) {
        List<AbstractJob> jobs = jobRepo.findByJobNameAndDeleted(jobName, false);
        if (jobs.size() == 0) {
            LOGGER.warn("Job name {} does not exist.", jobName);
            throw new GriffinException.NotFoundException(JOB_NAME_DOES_NOT_EXIST);
        }
        AbstractJob job = jobs.get(0);
        return getJobSchedule(job);
    }

    @Override
    public JobSchedule getJobSchedule(Long jobId) {
        AbstractJob job = jobRepo.findByIdAndDeleted(jobId, false);
        if (job == null) {
            LOGGER.warn("Job id {} does not exist.", jobId);
            throw new GriffinException.NotFoundException(JOB_ID_DOES_NOT_EXIST);
        }
        return getJobSchedule(job);
    }

    private JobSchedule getJobSchedule(AbstractJob job) {
        JobSchedule jobSchedule = job.getJobSchedule();
        jobSchedule.setId(job.getId());
        return jobSchedule;
    }

    @Override
    public JobSchedule addJob(JobSchedule js) throws Exception {
        Long measureId = js.getMeasureId();
        GriffinMeasure measure = getMeasureIfValid(measureId);
        JobOperator op = getJobOperator(measure.getProcessType());
        return op.add(js, measure);
    }

    /**
     * @param jobId  job id
     * @param action job operation: start job, stop job
     */
    @Override
    public JobDataBean onAction(Long jobId, String action) throws Exception {
        AbstractJob job = jobRepo.findByIdAndDeleted(jobId, false);
        validateJobExist(job);
        JobOperator op = getJobOperator(job);
        doAction(action, job, op);
        return genJobDataBean(job,action);
    }

    private void doAction(String action, AbstractJob job, JobOperator op) throws Exception {
        switch (action) {
            case START:
                op.start(job);
                break;
            case STOP:
                op.stop(job);
                break;
            default:
                throw new GriffinException.NotFoundException(NO_SUCH_JOB_ACTION);
        }
    }


    /**
     * logically delete
     * 1. pause these jobs
     * 2. set these jobs as deleted status
     *
     * @param jobId griffin job id
     */
    @Override
    public void deleteJob(Long jobId) throws SchedulerException {
        AbstractJob job = jobRepo.findByIdAndDeleted(jobId, false);
        validateJobExist(job);
        JobOperator op = getJobOperator(job);
        op.delete(job);
    }

    /**
     * logically delete
     *
     * @param name griffin job name which may not be unique.
     */
    @Override
    public void deleteJob(String name) throws SchedulerException {
        List<AbstractJob> jobs = jobRepo.findByJobNameAndDeleted(name, false);
        if (CollectionUtils.isEmpty(jobs)) {
            LOGGER.warn("There is no job with '{}' name.", name);
            throw new GriffinException.NotFoundException(JOB_NAME_DOES_NOT_EXIST);
        }
        for (AbstractJob job : jobs) {
            JobOperator op = getJobOperator(job);
            op.delete(job);
        }
    }

    @Override
    public List<JobInstanceBean> findInstancesOfJob(Long jobId, int page, int size) {
        AbstractJob job = jobRepo.findByIdAndDeleted(jobId, false);
        if (job == null) {
            LOGGER.warn("Job id {} does not exist.", jobId);
            throw new GriffinException.NotFoundException(JOB_ID_DOES_NOT_EXIST);
        }
        size = size > MAX_PAGE_SIZE ? MAX_PAGE_SIZE : size;
        size = size <= 0 ? DEFAULT_PAGE_SIZE : size;
        Pageable pageable = new PageRequest(page, size, Sort.Direction.DESC, "tms");
        List<JobInstanceBean> instances = instanceRepo.findByJobId(jobId, pageable);
        return updateState(instances);
    }

    private List<JobInstanceBean> updateState(List<JobInstanceBean> instances) {
        for (JobInstanceBean instance : instances) {
            State state = instance.getState();
            if (state.equals(UNKNOWN) || isActive(state)) {
                syncInstancesOfJob(instance);
            }
        }
        return instances;
    }

    /**
     * a job is regard as healthy job when its latest instance is in healthy state.
     *
     * @return job healthy statistics
     */
    @Override
    public JobHealth getHealthInfo() {
        JobHealth jobHealth = new JobHealth();
        List<AbstractJob> jobs = jobRepo.findByDeleted(false);
        for (AbstractJob job : jobs) {
            JobOperator op = getJobOperator(job);
            try {
                jobHealth = op.getHealth(jobHealth, job);
            } catch (SchedulerException e) {
                LOGGER.error("Job schedule exception. {}", e.getMessage());
                throw new GriffinException.ServiceException("Fail to Get HealthInfo", e);
            }

        }
        return jobHealth;
    }

    @Scheduled(fixedDelayString = "${jobInstance.expired.milliseconds}")
    public void deleteExpiredJobInstance() {
        Long timeMills = System.currentTimeMillis();
        List<JobInstanceBean> instances = instanceRepo.findByExpireTmsLessThanEqual(timeMills);
        if (!batchJobOp.pauseJobInstances(instances)) {
            LOGGER.error("Pause job failure.");
            return;
        }
        int count = instanceRepo.deleteByExpireTimestamp(timeMills);
        LOGGER.info("Delete {} expired job instances.", count);
    }

    private void validateJobExist(AbstractJob job) {
        if (job == null) {
            LOGGER.warn("Griffin job does not exist.");
            throw new GriffinException.NotFoundException(JOB_ID_DOES_NOT_EXIST);
        }
    }

    private JobOperator getJobOperator(AbstractJob job) {
        if (job instanceof BatchJob) {
            return batchJobOp;
        } else if (job instanceof StreamingJob) {
            return streamingJobOp;
        }
        throw new GriffinException.BadRequestException(JOB_TYPE_DOES_NOT_SUPPORT);
    }

    private JobOperator getJobOperator(ProcessType type) {
        if (type == BATCH) {
            return batchJobOp;
        } else if (type == STREAMING) {
            return streamingJobOp;
        }
        throw new GriffinException.BadRequestException(MEASURE_TYPE_DOES_NOT_SUPPORT);
    }

    TriggerKey getTriggerKeyIfValid(String qName, String qGroup) throws SchedulerException {
        TriggerKey triggerKey = triggerKey(qName, qGroup);
        if (factory.getScheduler().checkExists(triggerKey)) {
            throw new GriffinException.ConflictException(QUARTZ_JOB_ALREADY_EXIST);
        }
        return triggerKey;
    }

    List<? extends Trigger> getTriggers(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        Scheduler scheduler = factory.getScheduler();
        return scheduler.getTriggersOfJob(jobKey);
    }

    private JobDataBean genJobDataBean(AbstractJob job, String action) throws SchedulerException {
        if (job.getName() == null || job.getGroup() == null) {
            return null;
        }
        JobDataBean jobData = new JobDataBean();
        List<? extends Trigger> triggers = getTriggers(job.getName(), job.getGroup());
        /* If triggers are empty, in Griffin it means job is not scheduled or completed whose trigger state is NONE. */
        if (CollectionUtils.isEmpty(triggers) && job instanceof BatchJob) {
            return null;
        }
        setTriggerTime(triggers, jobData);
        JobOperator op = getJobOperator(job);
        JobState state = op.getState(job, jobData, action);
        jobData.setJobState(state);
        jobData.setJobId(job.getId());
        jobData.setJobName(job.getJobName());
        jobData.setMeasureId(job.getMeasureId());
        jobData.setCronExpression(getCronExpression(triggers));
        jobData.setProcessType(job instanceof BatchJob ? BATCH : STREAMING);
        return jobData;
    }

    private JobDataBean genJobDataBean(AbstractJob job) throws SchedulerException {
        return genJobDataBean(job,null);
    }

    private void setTriggerTime(List<? extends Trigger> triggers, JobDataBean jobBean) {
        if (CollectionUtils.isEmpty(triggers)) {
            return;
        }
        Trigger trigger = triggers.get(0);
        Date nextFireTime = trigger.getNextFireTime();
        Date previousFireTime = trigger.getPreviousFireTime();
        jobBean.setNextFireTime(nextFireTime != null ? nextFireTime.getTime() : -1);
        jobBean.setPreviousFireTime(previousFireTime != null ? previousFireTime.getTime() : -1);
    }

    private String getCronExpression(List<? extends Trigger> triggers) {
        for (Trigger trigger : triggers) {
            if (trigger instanceof CronTrigger) {
                return ((CronTrigger) trigger).getCronExpression();
            }
        }
        return null;
    }

    void addJob(TriggerKey tk, JobSchedule js, AbstractJob job, ProcessType type) throws Exception {
        JobDetail jobDetail = addJobDetail(tk, job);
        Trigger trigger = genTriggerInstance(tk, jobDetail, js, type);
        factory.getScheduler().scheduleJob(trigger);
    }

    String getQuartzName(JobSchedule js) {
        return js.getJobName() + "_" + System.currentTimeMillis();
    }

    String getQuartzGroup() {
        return "BA";
    }

    boolean isValidJobName(String jobName) {
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


    private GriffinMeasure getMeasureIfValid(Long measureId) {
        GriffinMeasure measure = measureRepo.findByIdAndDeleted(measureId, false);
        if (measure == null) {
            LOGGER.warn("The measure id {} isn't valid. Maybe it doesn't exist or is external measure type.", measureId);
            throw new GriffinException.BadRequestException(INVALID_MEASURE_ID);
        }
        return measure;
    }

    private Trigger genTriggerInstance(TriggerKey tk, JobDetail jd, JobSchedule js, ProcessType type) {
        TriggerBuilder builder = newTrigger().withIdentity(tk).forJob(jd);
        if (type == BATCH) {
            TimeZone timeZone = getTimeZone(js.getTimeZone());
            return builder.withSchedule(cronSchedule(js.getCronExpression()).inTimeZone(timeZone)).build();
        } else if (type == STREAMING) {
            return builder.startNow().withSchedule(simpleSchedule().withRepeatCount(0)).build();
        }
        throw new GriffinException.BadRequestException(JOB_TYPE_DOES_NOT_SUPPORT);

    }

    private JobDetail addJobDetail(TriggerKey triggerKey, AbstractJob job) throws SchedulerException {
        Scheduler scheduler = factory.getScheduler();
        JobKey jobKey = jobKey(triggerKey.getName(), triggerKey.getGroup());
        JobDetail jobDetail;
        Boolean isJobKeyExist = scheduler.checkExists(jobKey);
        if (isJobKeyExist) {
            jobDetail = scheduler.getJobDetail(jobKey);
        } else {
            jobDetail = newJob(JobInstance.class).storeDurably().withIdentity(jobKey).build();
        }
        setJobDataMap(jobDetail, job);
        scheduler.addJob(jobDetail, isJobKeyExist);
        return jobDetail;
    }

    private void setJobDataMap(JobDetail jd, AbstractJob job) {
        JobDataMap jobDataMap = jd.getJobDataMap();
        jobDataMap.put(GRIFFIN_JOB_ID, job.getId().toString());
    }


    /**
     * deleteJobsRelateToMeasure
     * 1. search jobs related to measure
     * 2. deleteJob
     *
     * @param measureId measure id
     */
    public void deleteJobsRelateToMeasure(Long measureId) throws SchedulerException {
        List<AbstractJob> jobs = jobRepo.findByMeasureIdAndDeleted(measureId, false);
        if (CollectionUtils.isEmpty(jobs)) {
            LOGGER.info("Measure id {} has no related jobs.", measureId);
            return;
        }
        for (AbstractJob job : jobs) {
            JobOperator op = getJobOperator(job);
            op.delete(job);
        }
    }

    @Scheduled(fixedDelayString = "${jobInstance.fixedDelay.in.milliseconds}")
    public void syncInstancesOfAllJobs() {
        LivySessionStates.State[] states = {STARTING, NOT_STARTED, RECOVERING, IDLE, RUNNING, BUSY};
        List<JobInstanceBean> beans = instanceRepo.findByActiveState(states);
        for (JobInstanceBean jobInstance : beans) {
            syncInstancesOfJob(jobInstance);
        }
    }

    /**
     * call livy to update part of job instance table data associated with group and jobName in mysql.
     *
     * @param instance job instance livy info
     */
    private void syncInstancesOfJob(JobInstanceBean instance) {
        if (instance.getSessionId() == null) {
            return;
        }
        String uri = livyConf.getProperty("livy.uri") + "/" + instance.getSessionId();
        TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {
        };
        try {
            String resultStr = restTemplate.getForObject(uri, String.class);
            HashMap<String, Object> resultMap = JsonUtil.toEntity(resultStr, type);
            setJobInstanceIdAndUri(instance, resultMap);
        } catch (ResourceAccessException e) {
            LOGGER.error("Your url may be wrong. Please check {}.\n {}", uri, e.getMessage());
        } catch (HttpClientErrorException e) {
            LOGGER.warn("sessionId({}) appId({}) {}.", instance.getSessionId(), instance.getAppId(), e.getMessage());
            setStateByYarn(instance, e);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

    }

    private void setStateByYarn(JobInstanceBean instance, HttpClientErrorException e) {
        int code = e.getStatusCode().value();
        boolean match = (code == 400 || code == 404) && instance.getAppId() != null;
        //this means your url is correct,but your param is wrong or livy session may be overdue.
        if (match) {
            setStateByYarn(instance);
        }
    }

    private void setStateByYarn(JobInstanceBean instance) {
        LOGGER.warn("Spark session {} may be overdue! Now we use yarn to update state.", instance.getSessionId());
        String yarnUrl = livyConf.getProperty("spark.uri");
        boolean success = YarnNetUtil.update(yarnUrl, instance);
        if (!success) {
            if (instance.getState().equals(UNKNOWN)) {
                return;
            }
            instance.setState(UNKNOWN);
        }
        instanceRepo.save(instance);
    }


    private void setJobInstanceIdAndUri(JobInstanceBean instance, HashMap<String, Object> resultMap) {
        if (resultMap != null) {
            Object state = resultMap.get("state");
            Object appId = resultMap.get("appId");
            instance.setState(state == null ? null : LivySessionStates.State.valueOf(state.toString().toUpperCase()));
            instance.setAppId(appId == null ? null : appId.toString());
            instance.setAppUri(appId == null ? null : livyConf.getProperty("spark.uri") + "/cluster/app/" + appId);
            instanceRepo.save(instance);
        }
    }

    public Boolean isJobHealthy(Long jobId) {
        Pageable pageable = new PageRequest(0, 1, Sort.Direction.DESC, "tms");
        List<JobInstanceBean> instances = instanceRepo.findByJobId(jobId, pageable);
        return !CollectionUtils.isEmpty(instances) && LivySessionStates.isHealthy(instances.get(0).getState());
    }
}
