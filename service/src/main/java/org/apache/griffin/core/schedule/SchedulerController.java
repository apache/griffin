/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

package org.apache.griffin.core.schedule;

import org.apache.griffin.core.schedule.Repo.ScheduleStateRepo;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.Serializable;
import java.text.ParseException;
import java.util.*;

import static org.quartz.DateBuilder.futureDate;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

@RestController
@RequestMapping("/jobs")
public class SchedulerController {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerController.class);

    @Autowired
    private SchedulerFactoryBean factory;
    @Autowired
    private ScheduleStateRepo scheduleStateRepo;

    public static final String ACCURACY_BATCH_GROUP = "BA";

    @RequestMapping("/")
    public List<Map<String, Serializable>> jobs() throws SchedulerException,
            ParseException {
        Scheduler scheduler = factory.getObject();

        List<Map<String, Serializable>> list = new ArrayList<>();
        for (String groupName : scheduler.getJobGroupNames()) {
            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher
                    .jobGroupEquals(groupName))) {
                String jobName = jobKey.getName();
                String jobGroup = jobKey.getGroup();

                JobDetail jd = scheduler.getJobDetail(jobKey);

                List<Trigger> triggers = (List<Trigger>) scheduler
                        .getTriggersOfJob(jobKey);
                Map<String, Serializable> map = new HashMap<>();

                if (triggers.size() > 0) {
                    //always get next run
                    Date nextFireTime = triggers.get(0).getNextFireTime();
                    Date previousFireTime=triggers.get(0).getPreviousFireTime();
//                    triggers.get(0).getScheduleBuilder()

                    Trigger.TriggerState triggerState=scheduler.getTriggerState(triggers.get(0).getKey());

                    map.put("jobName", jobName);
                    map.put("groupName", jobGroup);
                    map.put("nextFireTime", nextFireTime.getTime());
                    if (previousFireTime!=null)
                        map.put("previousFireTime",previousFireTime.getTime());
                    else
                        map.put("previousFireTime",-1);
                    map.put("triggerState",triggerState);

                    map.put("measure", (String) jd.getJobDataMap().get("measure"));
                    map.put("sourcePat",jd.getJobDataMap().getString("sourcePat"));
                    map.put("targetPat",jd.getJobDataMap().getString("targetPat"));

                    map.put("dataStartTimestamp",Long.parseLong(jd.getJobDataMap().getString("dataStartTimestamp")));
                    map.put("jobStartTime",jd.getJobDataMap().getString("jobStartTime"));
                    map.put("periodTime",jd.getJobDataMap().getString("periodTime"));
                    list.add(map);
                }

            }
        }
        return list;
    }

    @RequestMapping(value = "/{groupName}/{jobName}", method = RequestMethod.DELETE)
    public String removeTask(@PathVariable String groupName, @PathVariable String jobName)
            throws SchedulerException {
        Scheduler scheduler = factory.getObject();
        JobKey jobKey = new JobKey(jobName, groupName);
        scheduler.deleteJob(jobKey);
        return "task has been removed";
    }

    //@RequestMapping(value = "/add/{groupName}/{jobName}/{measureName}/{sourcePat}/{targetPat}")
    @RequestMapping(value = "/add/{groupName}/{jobName}/{measureName}", method = RequestMethod.POST)
    @ResponseBody
    @Produces(MediaType.APPLICATION_JSON)
    public Boolean addTask(@PathVariable String groupName,
                           @PathVariable String jobName,
                           @PathVariable String measureName,
                           @RequestBody SchedulerRequestBody schedulerRequestBody) {
        //@RequestParam(value = "sourcePat", required = false) String sourcePat,
        //@RequestParam(value = "targetPat", required = false) String targetPat
        int jobStartTime = 0;
        int periodTime = 0;
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
                scheduler.addJob(jobDetail, true);
            } else {
                jobDetail = newJob(SparkSubmitJob.class)
                        .storeDurably()
                        .withIdentity(jobKey)
                        .build();

                jobDetail.getJobDataMap().put("measure", measureName);
                jobDetail.getJobDataMap().put("sourcePat", schedulerRequestBody.getSourcePat());
                jobDetail.getJobDataMap().put("targetPat", schedulerRequestBody.getTargetPat());
                jobDetail.getJobDataMap().put("dataStartTimestamp", schedulerRequestBody.getDataStartTimestamp());
                jobDetail.getJobDataMap().put("jobStartTime", schedulerRequestBody.getJobStartTime());
                jobDetail.getJobDataMap().put("periodTime", schedulerRequestBody.getPeriodTime());
                jobDetail.getJobDataMap().put("lastTime", "");
                jobDetail.getJobDataMap().put("groupName",groupName);
                jobDetail.getJobDataMap().put("jobName",jobName);

                jobStartTime = Integer.parseInt(schedulerRequestBody.getJobStartTime());
                periodTime = Integer.parseInt(schedulerRequestBody.getPeriodTime());
                scheduler.addJob(jobDetail, false);
            }

            Trigger trigger = newTrigger()
                    .withIdentity(triggerKey)
                    .forJob(jobDetail)
                    //hardcode to
//					.withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 0 * * ?"))
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInSeconds(periodTime)
                            .repeatForever())
                    .startAt(futureDate(jobStartTime, DateBuilder.IntervalUnit.SECOND))
                    .build();

            scheduler.scheduleJob(trigger);

            return true;

        } catch (SchedulerException e) {
            LOGGER.error("", e);
            return false;
        }
    }


    @RequestMapping(value = "/groups/{group}/jobs/{name}", method = RequestMethod.DELETE)
    public boolean deleteJob(@PathVariable String group, @PathVariable String name) {
        try {
            Scheduler scheduler = factory.getObject();
            scheduler.deleteJob(new JobKey(name, group));
            return true;
        } catch (SchedulerException e) {
            LOGGER.error(e.getMessage());
            return false;
        }
    }

    @RequestMapping("/instances/groups/{group}/jobs/{name}/{page}/{size}")
    public List<ScheduleState> findInstancesOfJob(@PathVariable String group,@PathVariable String name,@PathVariable int page,@PathVariable int size){
        Pageable pageRequest=new PageRequest(page,size, Sort.Direction.DESC,"timestamp");
        return scheduleStateRepo.findByGroupNameAndJobName(group,name,pageRequest);
    }

    @RequestMapping("/statics")
    public JobHealth getHealthInfo() throws SchedulerException {
        Scheduler scheduler=factory.getObject();
        int jobCount=scheduler.getJobGroupNames().size();
        int health=0;
        int invalid=0;
        for (String groupName : scheduler.getJobGroupNames()){
            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))){
                String jobName=jobKey.getName();
                String jobGroup=jobKey.getGroup();
                Pageable pageRequest=new PageRequest(0,1, Sort.Direction.DESC,"timestamp");
                ScheduleState scheduleState=scheduleStateRepo.findByGroupNameAndJobName(jobGroup,jobName,pageRequest).get(0);
                if(scheduleState.getState().equals("starting")){
                    health++;
                }else{
                    invalid++;
                }
            }
        }
        JobHealth jobHealth=new JobHealth(health,invalid,jobCount);
        return jobHealth;
    }
}


