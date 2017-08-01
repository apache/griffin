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
import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.JobRequestBody;
import org.apache.griffin.core.job.entity.LivySessionStateMap;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
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

import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

@Service
public class JobServiceImpl implements JobService {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceImpl.class);

    @Autowired
    SchedulerFactoryBean factory;
    @Autowired
    JobInstanceRepo jobInstanceRepo;

    public static final String ACCURACY_BATCH_GROUP = "BA";

    @Autowired
    Properties sparkJobProps;


    public JobServiceImpl(){
    }

    @Override
    public List<Map<String, Serializable>> getJobs() {
        Scheduler scheduler = factory.getObject();
        List<Map<String, Serializable>> list = new ArrayList<>();
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                    setJobsByKey(list,scheduler, jobKey);
                }
            }
        } catch (SchedulerException e) {
            LOGGER.error("failed to get jobs."+e);
        }
        return list;
    }

    public void setJobsByKey(List<Map<String, Serializable>> list,Scheduler scheduler, JobKey jobKey) throws SchedulerException {
        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
        if (triggers.size() > 0) {
            //always get next run
            Map<String, Serializable> map = new HashMap<>();
            setMap(map,scheduler,jobKey);
            list.add(map);
        }
    }


    public void setMap(Map<String, Serializable> map,Scheduler scheduler,JobKey jobKey) throws SchedulerException {
        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
        JobDetail jd = scheduler.getJobDetail(jobKey);
        Date nextFireTime = triggers.get(0).getNextFireTime();
        Date previousFireTime=triggers.get(0).getPreviousFireTime();
        Trigger.TriggerState triggerState=scheduler.getTriggerState(triggers.get(0).getKey());

        map.put("jobName", jobKey.getName());
        map.put("groupName", jobKey.getGroup());
        if (nextFireTime!=null){
            map.put("nextFireTime", nextFireTime.getTime());
        }
        else {
            map.put("nextFireTime", -1);
        }
        if (previousFireTime!=null) {
            map.put("previousFireTime", previousFireTime.getTime());
        }
        else {
            map.put("previousFireTime", -1);
        }
        map.put("triggerState",triggerState);
        map.put("measure", jd.getJobDataMap().getString("measure"));
        map.put("sourcePat",jd.getJobDataMap().getString("sourcePat"));
        map.put("targetPat",jd.getJobDataMap().getString("targetPat"));
        if(StringUtils.isNotEmpty(jd.getJobDataMap().getString("dataStartTimestamp"))) {
            map.put("dataStartTimestamp", jd.getJobDataMap().getString("dataStartTimestamp"));
        }
        map.put("jobStartTime",jd.getJobDataMap().getString("jobStartTime"));
        map.put("periodTime",jd.getJobDataMap().getString("periodTime"));
    }

    @Override
    public GriffinOperationMessage addJob(String groupName, String jobName, String measureName, JobRequestBody jobRequestBody) {
        int periodTime = 0;
        Date jobStartTime=null;
        try{
            periodTime = Integer.parseInt(jobRequestBody.getPeriodTime());
            jobStartTime=new Date(Long.parseLong(jobRequestBody.getJobStartTime()));
            setJobStartTime(jobStartTime,periodTime);
        }catch (Exception e){
            LOGGER.info("jobStartTime or periodTime format error! "+e);
            return GriffinOperationMessage.CREATE_JOB_FAIL;
        }
        try {
            Scheduler scheduler = factory.getObject();
            TriggerKey triggerKey = triggerKey(jobName, groupName);
            if (scheduler.checkExists(triggerKey)) {
                scheduler.unscheduleJob(triggerKey);
            }
            JobKey jobKey = jobKey(jobName, groupName);
            JobDetail jobDetail;
            if (scheduler.checkExists(jobKey)) {
                jobDetail = scheduler.getJobDetail(jobKey);
                setJobDetail(jobDetail, jobRequestBody,measureName,groupName,jobName);
                scheduler.addJob(jobDetail, true);
            } else {
                jobDetail = newJob(SparkSubmitJob.class)
                        .storeDurably()
                        .withIdentity(jobKey)
                        .build();
                //set JobDetail
                setJobDetail(jobDetail, jobRequestBody,measureName,groupName,jobName);
                scheduler.addJob(jobDetail, false);
            }
            Trigger trigger = newTrigger()
                    .withIdentity(triggerKey)
                    .forJob(jobDetail)
//					.withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 0 * * ?"))
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInSeconds(periodTime)
                            .repeatForever())
                    .startAt(jobStartTime)
                    .build();
            scheduler.scheduleJob(trigger);
            return GriffinOperationMessage.CREATE_JOB_SUCCESS;
        } catch (SchedulerException e) {
            LOGGER.error("", e);
            return GriffinOperationMessage.CREATE_JOB_FAIL;
        }
    }

    public void setJobStartTime(Date jobStartTime,int periodTime){
        long currentTimestamp=System.currentTimeMillis();
        long jobstartTimestamp=jobStartTime.getTime();
        //if jobStartTime is before currentTimestamp, set it as the latest trigger time in the future
        if(jobStartTime.before(new Date(currentTimestamp))){
            long n=(currentTimestamp-jobstartTimestamp)/(long)(periodTime*1000);
            jobstartTimestamp=jobstartTimestamp+(n+1)*(long)(periodTime*1000);
            jobStartTime.setTime(jobstartTimestamp);
        }
    }

    public void setJobDetail(JobDetail jobDetail, JobRequestBody jobRequestBody, String measureName, String groupName, String jobName){
        jobDetail.getJobDataMap().put("measure", measureName);
        jobDetail.getJobDataMap().put("sourcePat", jobRequestBody.getSourcePat());
        jobDetail.getJobDataMap().put("targetPat", jobRequestBody.getTargetPat());
        jobDetail.getJobDataMap().put("dataStartTimestamp", jobRequestBody.getDataStartTimestamp());
        jobDetail.getJobDataMap().put("jobStartTime", jobRequestBody.getJobStartTime());
        jobDetail.getJobDataMap().put("periodTime", jobRequestBody.getPeriodTime());
        jobDetail.getJobDataMap().put("lastTime", "");
        jobDetail.getJobDataMap().put("groupName",groupName);
        jobDetail.getJobDataMap().put("jobName",jobName);
    }

    @Override
    public GriffinOperationMessage deleteJob(String group, String name) {
        try {
            Scheduler scheduler = factory.getObject();
            scheduler.deleteJob(new JobKey(name, group));
            jobInstanceRepo.deleteInGroupAndjobName(group,name);
            return GriffinOperationMessage.DELETE_JOB_SUCCESS;
        } catch (SchedulerException e) {
            LOGGER.error(GriffinOperationMessage.DELETE_JOB_FAIL+""+e);
            return GriffinOperationMessage.DELETE_JOB_FAIL;
        }
    }

    @Override
    public List<JobInstance> findInstancesOfJob(String group, String jobName, int page, int size) {
        //query and return instances
        Pageable pageRequest=new PageRequest(page,size, Sort.Direction.DESC,"timestamp");
        return jobInstanceRepo.findByGroupNameAndJobName(group,jobName,pageRequest);
    }

    public void syncInstancesOfJob(String group, String jobName) {
        //update all instance info belongs to this group and job.
        List<JobInstance> jobInstanceList=jobInstanceRepo.findByGroupNameAndJobName(group,jobName);
        for (JobInstance jobInstance:jobInstanceList){
            if (!LivySessionStateMap.isActive(jobInstance.getState().toString())){
                continue;
            }
            String uri=sparkJobProps.getProperty("sparkJob.uri")+"/"+jobInstance.getSessionId();
            RestTemplate restTemplate=new RestTemplate();
            String resultStr=null;
            try{
                resultStr=restTemplate.getForObject(uri,String.class);
            }catch (Exception e){
                LOGGER.error("spark session "+jobInstance.getSessionId()+" has overdue, set state as unknown!\n"+e);
                //if server cannot get session from Livy, set State as unknown.
                jobInstance.setState(LivySessionStateMap.State.unknown);
            }
            TypeReference<HashMap<String,Object>> type=new TypeReference<HashMap<String,Object>>(){};
            HashMap<String,Object> resultMap= null;
            try {
                resultMap = GriffinUtil.toEntity(resultStr,type);
            } catch (IOException e) {
                LOGGER.error("jobInstance jsonStr convert to map failed. "+e);
                continue;
            }
            try{
                if (resultMap!=null && resultMap.size()!=0){
                    jobInstance.setState(LivySessionStateMap.State.valueOf(resultMap.get("state").toString()));
                    jobInstance.setAppId(resultMap.get("appId").toString());
                }
            }catch (Exception e){
                LOGGER.warn(group+","+jobName+"job Instance has some null field (state or appId). "+e);
                continue;
            }
            jobInstanceRepo.setFixedStateAndappIdFor(jobInstance.getId(),jobInstance.getState(),jobInstance.getAppId());
        }
    }

    @Scheduled(fixedDelayString = "${jobInstance.fixedDelay.in.milliseconds}")
    public void syncInstancesOfAllJobs(){
        List<Object> groupJobList=jobInstanceRepo.findGroupWithJobName();
        for (Object groupJobObj : groupJobList){
            try{
                Object[] groupJob=(Object[])groupJobObj;
                if (groupJob!=null && groupJob.length==2){
                    syncInstancesOfJob(groupJob[0].toString(),groupJob[1].toString());
                }
            }catch (Exception e){
                LOGGER.error("schedule update instances of all jobs failed. "+e);
            }
        }
    }

    @Override
    public JobHealth getHealthInfo()  {
        Scheduler scheduler=factory.getObject();
        int jobCount= 0;
        int notHealthyCount=0;
        try {
            for (String groupName : scheduler.getJobGroupNames()){
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))){
                    jobCount++;
                    String jobName=jobKey.getName();
                    String jobGroup=jobKey.getGroup();
                    Pageable pageRequest=new PageRequest(0,1, Sort.Direction.DESC,"timestamp");
                    JobInstance latestJobInstance=new JobInstance();
                    if (jobInstanceRepo.findByGroupNameAndJobName(jobGroup,jobName,pageRequest)!=null
                            &&jobInstanceRepo.findByGroupNameAndJobName(jobGroup,jobName,pageRequest).size()>0){
                        latestJobInstance=jobInstanceRepo.findByGroupNameAndJobName(jobGroup,jobName,pageRequest).get(0);
                        if(LivySessionStateMap.State.error.toString().equals(latestJobInstance.getState()) ||
                                LivySessionStateMap.State.dead.toString().equals(latestJobInstance.getState()) ||
                                LivySessionStateMap.State.shutting_down.equals(latestJobInstance.getState())
                                ){
                            notHealthyCount++;
                        }
                    }
                }
            }
        } catch (SchedulerException e) {
            LOGGER.error(""+e);
        }
        JobHealth jobHealth=new JobHealth(jobCount-notHealthyCount,jobCount);
        return jobHealth;
    }
}
