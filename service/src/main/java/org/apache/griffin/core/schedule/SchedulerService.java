package org.apache.griffin.core.schedule;

import org.quartz.SchedulerException;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by xiangrchen on 6/7/17.
 */
public interface SchedulerService {
    public List<Map<String, Serializable>> getJobs() throws SchedulerException;

    public Boolean addJob(String groupName,String jobName,String measureName,SchedulerRequestBody schedulerRequestBody);

    public Boolean deleteJob(String groupName,String jobName);

    public List<ScheduleState> findInstancesOfJob(String group,String name,int page,int size);

    public JobHealth getHealthInfo() throws SchedulerException;


}
