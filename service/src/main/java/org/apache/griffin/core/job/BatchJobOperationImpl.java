package org.apache.griffin.core.job;

import org.apache.griffin.core.exception.GriffinException;
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.job.repo.GriffinJobRepo;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;

import static org.apache.griffin.core.exception.GriffinExceptionMessage.*;
import static org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType.batch;
import static org.quartz.CronExpression.isValidExpression;
import static org.quartz.JobKey.jobKey;
import static org.quartz.Trigger.TriggerState.PAUSED;

@Service
public class BatchJobOperationImpl implements JobOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchJobOperationImpl.class);

    @Autowired
    private SchedulerFactoryBean factory;
    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    private GriffinJobRepo batchJobRepo;
    @Autowired
    private JobServiceImpl jobService;

    //TODO finally you should validate transactional whether working

    @Override
    @Transactional(rollbackFor = Exception.class)
    public AbstractJob add(JobSchedule js, GriffinMeasure measure) throws Exception {
        validateParams(js, measure);
        String qName = jobService.getQuartzName(js);
        String qGroup = jobService.getQuartzGroup();
        TriggerKey triggerKey = jobService.getTriggerKeyIfValid(qName, qGroup);
        BatchJob batchJob = new BatchJob(js.getMeasureId(), js.getJobName(), qName, qGroup, false);
        batchJob.setJobSchedule(js);
        batchJobRepo.save(batchJob);
        jobService.addJob(triggerKey, js, batchJob, batch);
        return batchJob;
    }

    @Override
    public void start(AbstractJob job) {
        BatchJob batchJob = (BatchJob) job;
        Trigger.TriggerState state = getTriggerStateIfValid(batchJob);
        //If job is not in paused state,we can't start it as it may be running.
        if (state != PAUSED) {
            throw new GriffinException.BadRequestException(JOB_IS_NOT_IN_PAUSED_STATUS);
        }
        JobKey jobKey = jobKey(batchJob.getQuartzName(), batchJob.getQuartzGroup());
        try {
            factory.getScheduler().resumeJob(jobKey);
        } catch (SchedulerException e) {
            throw new GriffinException.ServiceException("Failed to start job.", e);
        }
    }

    @Override
    public void stop(AbstractJob job) {
        pauseJob((BatchJob) job, false);
    }

    @Override
    public void delete(AbstractJob job) {
        pauseJob((BatchJob) job, true);
    }

    @Override
    public JobDataBean getJobData(AbstractJob job) throws SchedulerException {
        BatchJob batchJob = (BatchJob) job;
        Scheduler scheduler = factory.getScheduler();
        JobKey jobKey = jobKey(batchJob.getQuartzName(), batchJob.getQuartzGroup());
        List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
        if (CollectionUtils.isEmpty(triggers)) {
            LOGGER.info("Job({},{}) isn't scheduled.", job.getId(), job.getJobName());
            return null;
        }
        return gebJobDataBean(job, scheduler, triggers);
    }

    @Override
    public JobHealth getHealthInfo(JobHealth jobHealth, AbstractJob job) {
        List<? extends Trigger> triggers = getTriggers((BatchJob) job);
        if (!CollectionUtils.isEmpty(triggers)) {
            jobHealth.setJobCount(jobHealth.getJobCount() + 1);
            if (jobService.isJobHealthy(job.getId())) {
                jobHealth.setHealthyJobCount(jobHealth.getHealthyJobCount() + 1);
            }
        }
        return jobHealth;
    }

    private List<? extends Trigger> getTriggers(BatchJob job) {
        JobKey jobKey = new JobKey(job.getQuartzName(), job.getQuartzGroup());
        List<? extends Trigger> triggers;
        try {
            triggers = factory.getScheduler().getTriggersOfJob(jobKey);
        } catch (SchedulerException e) {
            LOGGER.error("Job schedule exception. {}", e.getMessage());
            throw new GriffinException.ServiceException("Fail to Get HealthInfo", e);
        }
        return triggers;
    }

    private JobDataBean gebJobDataBean(AbstractJob job, Scheduler scheduler, List<? extends Trigger> triggers) throws SchedulerException {
        JobDataBean jobData = new JobDataBean();
        Trigger trigger = triggers.get(0);
        setTriggerTime(trigger, jobData);
        jobData.setJobId(job.getId());
        jobData.setJobName(job.getJobName());
        jobData.setMeasureId(job.getMeasureId());
        jobData.setState(scheduler.getTriggerState(trigger.getKey()).toString());
        jobData.setCronExpression(getCronExpression(triggers));
        jobData.setType(batch);
        return jobData;
    }

    private void setTriggerTime(Trigger trigger, JobDataBean jobBean) {
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

    private Trigger.TriggerState getTriggerStateIfValid(BatchJob batchJob) {
        Scheduler scheduler = factory.getScheduler();
        JobKey jobKey = jobKey(batchJob.getQuartzName(), batchJob.getQuartzGroup());
        List<? extends Trigger> triggers;
        try {
            triggers = scheduler.getTriggersOfJob(jobKey);
            if (CollectionUtils.isEmpty(triggers)) {
                throw new GriffinException.BadRequestException(JOB_IS_NOT_SCHEDULED);
            }
            return scheduler.getTriggerState(triggers.get(0).getKey());
        } catch (SchedulerException e) {
            LOGGER.error("Failed to delete job", e);
            throw new GriffinException.ServiceException("Failed to delete job", e);
        }

    }


    /**
     * @param job          griffin job
     * @param isNeedDelete if job needs to be deleted,set isNeedDelete true,otherwise it just will be paused.
     */
    private void pauseJob(BatchJob job, boolean isNeedDelete) {
        try {
            pauseJob(job.getQuartzGroup(), job.getQuartzName());
            pausePredicateJob(job);
            if (isNeedDelete) {
                job.setDeleted(true);
            }
            batchJobRepo.save(job);
        } catch (Exception e) {
            LOGGER.error("Job schedule happens exception.", e);
            throw new GriffinException.ServiceException("Job schedule happens exception.", e);
        }
    }

    private void pausePredicateJob(BatchJob job) throws SchedulerException {
        List<JobInstanceBean> instances = job.getJobInstances();
        for (JobInstanceBean instance : instances) {
            if (!instance.getPredicateDeleted()) {
                deleteJob(instance.getPredicateGroup(), instance.getPredicateName());
                instance.setPredicateDeleted(true);
                if (instance.getState().equals(LivySessionStates.State.finding)) {
                    instance.setState(LivySessionStates.State.not_found);
                }
            }
        }
    }

    public void deleteJob(String group, String name) throws SchedulerException {
        Scheduler scheduler = factory.getScheduler();
        JobKey jobKey = new JobKey(name, group);
        if (!scheduler.checkExists(jobKey)) {
            LOGGER.info("Job({},{}) does not exist.", jobKey.getGroup(), jobKey.getName());
            return;
        }
        scheduler.deleteJob(jobKey);

    }

    private void pauseJob(String group, String name) throws SchedulerException {
        if (StringUtils.isEmpty(group) || StringUtils.isEmpty(name)) {
            return;
        }
        Scheduler scheduler = factory.getScheduler();
        JobKey jobKey = new JobKey(name, group);
        if (!scheduler.checkExists(jobKey)) {
            LOGGER.warn("Job({},{}) does not exist.", jobKey.getGroup(), jobKey.getName());
            return;
        }
        scheduler.pauseJob(jobKey);
    }

    public boolean pauseJobInstances(List<JobInstanceBean> instances) {
        if (CollectionUtils.isEmpty(instances)) {
            return true;
        }
        List<JobInstanceBean> deletedInstances = new ArrayList<>();
        boolean pauseStatus = true;
        for (JobInstanceBean instance : instances) {
            boolean status = pauseJobInstance(instance, deletedInstances);
            pauseStatus = pauseStatus && status;
        }
        jobInstanceRepo.save(deletedInstances);
        return pauseStatus;
    }

    private boolean pauseJobInstance(JobInstanceBean instance, List<JobInstanceBean> deletedInstances) {
        boolean status = true;
        String pGroup = instance.getPredicateGroup();
        String pName = instance.getPredicateName();
        try {
            if (!instance.getPredicateDeleted()) {
                deleteJob(pGroup, pName);
                instance.setPredicateDeleted(true);
                deletedInstances.add(instance);
            }
        } catch (SchedulerException e) {
            LOGGER.error("Failed to pause predicate job({},{}).", pGroup, pName);
            status = false;
        }
        return status;
    }

    private void validateParams(JobSchedule js, GriffinMeasure measure) {
        if (!jobService.isValidJobName(js.getJobName())) {
            throw new GriffinException.BadRequestException(INVALID_JOB_NAME);
        }
        if (!isValidCronExpression(js.getCronExpression())) {
            throw new GriffinException.BadRequestException(INVALID_CRON_EXPRESSION);
        }
        if (!isValidBaseLine(js.getSegments())) {
            throw new GriffinException.BadRequestException(MISSING_BASELINE_CONFIG);
        }
        List<String> names = getConnectorNames(measure);
        if (!isValidConnectorNames(js.getSegments(), names)) {
            throw new GriffinException.BadRequestException(INVALID_CONNECTOR_NAME);
        }
    }

    private boolean isValidCronExpression(String cronExpression) {
        if (org.apache.commons.lang.StringUtils.isEmpty(cronExpression)) {
            LOGGER.warn("Cron Expression is empty.");
            return false;
        }
        if (!isValidExpression(cronExpression)) {
            LOGGER.warn("Cron Expression is invalid.");
            return false;
        }
        return true;
    }

    private boolean isValidBaseLine(List<JobDataSegment> segments) {
        assert segments != null;
        for (JobDataSegment jds : segments) {
            if (jds.getBaseline()) {
                return true;
            }
        }
        LOGGER.warn("Please set segment timestamp baseline in as.baseline field.");
        return false;
    }

    private boolean isValidConnectorNames(List<JobDataSegment> segments, List<String> names) {
        assert segments != null;
        Set<String> sets = new HashSet<>();
        for (JobDataSegment segment : segments) {
            String dcName = segment.getDataConnectorName();
            sets.add(dcName);
            boolean exist = names.stream().anyMatch(name -> name.equals(dcName));
            if (!exist) {
                LOGGER.warn("Param {} is a illegal string. Please input one of strings in {}.", dcName, names);
                return false;
            }
        }
        if (sets.size() < segments.size()) {
            LOGGER.warn("Connector names in job data segment cannot duplicate.");
            return false;
        }
        return true;
    }

    private List<String> getConnectorNames(GriffinMeasure measure) {
        Set<String> sets = new HashSet<>();
        List<DataSource> sources = measure.getDataSources();
        for (DataSource source : sources) {
            source.getConnectors().forEach(dc -> sets.add(dc.getName()));
        }
        if (sets.size() < sources.size()) {
            LOGGER.warn("Connector names cannot be repeated.");
            return Collections.emptyList();
        }
        return new ArrayList<>(sets);
    }
}
