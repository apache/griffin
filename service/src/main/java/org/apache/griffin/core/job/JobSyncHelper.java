package org.apache.griffin.core.job;

import org.apache.griffin.core.job.entity.VirtualJob;
import org.apache.griffin.core.job.repo.JobRepo;
import org.apache.griffin.core.measure.entity.ExternalMeasure;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JobSyncHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobSyncHelper.class);

    @Autowired
    private SchedulerFactoryBean factory;
    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepo<VirtualJob> jobRepo;

    public Boolean createVirtualJob(ExternalMeasure measure) {
        if (jobRepo.findByMeasureIdAndDeleted(measure.getId(), false).size() != 0) {
            LOGGER.error("Failed to create new virtual job related to measure {}, it already exists.", measure.getName());
            return false;
        }
        String name = "virtual_".concat(measure.getName());
        if (jobRepo.findByNameAndDeleted(name, false).size() != 0) {
            LOGGER.error("Failed to create new virtual job {}, it already exists.", name);
            return false;
        }
        VirtualJob job = new VirtualJob(name, measure.getId(), measure.getMetricName());
        try {
            jobRepo.save(job);
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to save virtual job {}.", name, e.getMessage());
        }
        return false;
    }

    public Boolean updateVirtualJob(ExternalMeasure measure) {
        List<VirtualJob> jobList = jobRepo.findByMeasureIdAndDeleted(measure.getId(), false);
        switch (jobList.size()) {
            case 1:
                VirtualJob job = jobList.get(0);
                job.setName("virtual_".concat(measure.getName()));
                job.setMetricName(measure.getMetricName());
                jobRepo.save(job);
                LOGGER.info("Virtual job {} is updated.", job.getName());
                return true;
            case 0:
                LOGGER.error("Can't find the virtual job related to measure id {}.", measure.getId());
                return false;
            default:
                LOGGER.error("More than one virtual job related to measure id {} found.", measure.getId());
                return false;
        }

    }

    public Boolean deleteJobsRelateToMeasure(Measure measure) {
        if (measure instanceof GriffinMeasure) {
            return deleteGriffinJobs((GriffinMeasure) measure);
        } else {
            return deleteVirtualJobs((ExternalMeasure) measure);
        }
    }

    private Boolean deleteGriffinJobs(GriffinMeasure measure) {
        try {
            Scheduler scheduler = factory.getObject();
            //get all jobs
            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.anyGroup())) {
                JobDetail jobDetail = scheduler.getJobDetail(jobKey);
                JobDataMap jobDataMap = jobDetail.getJobDataMap();
                String measureId = jobDataMap.getString("measureId");
                if (measureId != null && measureId.equals(measure.getId().toString())) {
                    jobService.deleteJob(jobKey.getGroup(), jobKey.getName());
                    LOGGER.info("Griffin job {} is paused and logically deleted.", jobKey.getGroup(), jobKey.getName());
                }
            }
            return true;
        } catch (SchedulerException e) {
            LOGGER.error("{} {}", GriffinOperationMessage.PAUSE_JOB_FAIL, e.getMessage());
        }
        return false;
    }

    private Boolean deleteVirtualJobs(ExternalMeasure measure) {
        List<VirtualJob> jobList = jobRepo.findByMeasureIdAndDeleted(measure.getId(), false);
        switch (jobList.size()) {
            case 1:
                VirtualJob job = jobList.get(0);
                job.setDeleted(true);
                jobRepo.save(job);
                LOGGER.info("Virtual job {} is logically deleted.", job.getName());
                return true;
            case 0:
                LOGGER.error("Can't find the virtual job related to {}.", measure.getName());
                return false;
            default:
                LOGGER.error("More than one virtual job related to {} found.", measure.getName());
                return false;
        }
    }
}
