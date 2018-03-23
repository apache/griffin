package org.apache.griffin.core.job;

import org.apache.griffin.core.job.entity.AbstractJob;
import org.apache.griffin.core.job.entity.JobDataBean;
import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.job.entity.JobSchedule;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.quartz.SchedulerException;

public interface JobOperation {
    AbstractJob add(JobSchedule js, GriffinMeasure measure) throws Exception;

    void start(AbstractJob job);

    void stop(AbstractJob job);

    void delete(AbstractJob job);

    JobDataBean getJobData(AbstractJob job) throws SchedulerException;

    JobHealth getHealthInfo(JobHealth jobHealth, AbstractJob job);
}
