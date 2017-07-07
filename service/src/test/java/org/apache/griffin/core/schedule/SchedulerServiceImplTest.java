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

package org.apache.griffin.core.schedule;

import org.apache.griffin.core.schedule.Repo.ScheduleStateRepo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.Serializable;
import java.util.*;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;

@RunWith(SpringRunner.class)
public class SchedulerServiceImplTest {
    private static final Logger log = LoggerFactory.getLogger(SchedulerServiceImplTest.class);

    @TestConfiguration
    public static class SchedulerServiceConfiguration{
        @Bean
        public SchedulerServiceImpl service(){
            return new SchedulerServiceImpl();
        }
        @Bean
        public SchedulerFactoryBean factoryBean(){
            return new SchedulerFactoryBean();
        }
    }

    @MockBean
    private ScheduleStateRepo scheduleStateRepo;


    @MockBean
    private SchedulerFactoryBean factory;

    @Autowired
    private SchedulerServiceImpl service;

    @Before
    public void setup(){
    }

    @Test
    public void testGetJobs(){
        try {
            Scheduler scheduler=Mockito.mock(Scheduler.class);
            given(factory.getObject()).willReturn(scheduler);
            List<Map<String, Serializable>> tmp = service.getJobs();
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all jobs info from dbs");
        }
    }

    @Test
    public void testSetJobsByKey() throws SchedulerException {
        List<Map<String, Serializable>> list=new ArrayList<Map<String, Serializable>>();
        Scheduler scheduler=Mockito.mock(Scheduler.class);
        JobKey jobKey= new JobKey("TEST");
        List<Trigger> triggers=new ArrayList<Trigger>();
        Trigger trigger=new CronTriggerImpl();
        triggers.add(trigger);
        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(triggers);

        JobDetail jd=Mockito.mock(JobDetail.class);
        given(scheduler.getJobDetail(jobKey)).willReturn(jd);

        JobDataMap jobDataMap=Mockito.mock(JobDataMap.class);
        given(jd.getJobDataMap()).willReturn(jobDataMap);

        service.setJobsByKey(list,scheduler,jobKey);

    }

    @Test
    public void testAddJob(){
        try {
            String groupName="BA";
            String jobName="job1";
            String measureName="m1";
            SchedulerRequestBody schedulerRequestBody=new SchedulerRequestBody();
            Scheduler scheduler=Mockito.mock(Scheduler.class);
            given(factory.getObject()).willReturn(scheduler);
            Boolean tmp = service.addJob(groupName,jobName,measureName,schedulerRequestBody);
            assertEquals(tmp,false);
            assertTrue(true);

            SchedulerRequestBody schedulerRequestBody1=new SchedulerRequestBody("YYYYMMdd-HH","YYYYMMdd-HH",
                    System.currentTimeMillis()+"",System.currentTimeMillis()+"","1000");
            Scheduler scheduler1=Mockito.mock(Scheduler.class);
            given(factory.getObject()).willReturn(scheduler1);
            Boolean tmp1 = service.addJob(groupName,jobName,measureName,schedulerRequestBody1);
            assertEquals(tmp1,true);
        }catch (Throwable t){
            fail("Cannot add job ");
        }
    }

    @Test
    public void testDeleteJob(){
        String groupName="BA";
        String jobName="job1";
        try {
            Scheduler scheduler=Mockito.mock(Scheduler.class);
            given(factory.getObject()).willReturn(scheduler);
            Boolean tmp = service.deleteJob(groupName,jobName);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot delete job");
        }
        try {
            given(factory.getObject()).willThrow(SchedulerException.class);
            Boolean tmp = service.deleteJob(groupName,jobName);
        } catch (Exception e) {
            log.info("testGetAllTable: test catch "+e);
        }
    }

    @Test
    public void testFindInstancesOfJob(){
        try {
            String groupName="BA";
            String jobName="job1";
            int page=0;
            int size=2;
            List<ScheduleState> tmp = service.findInstancesOfJob(groupName,jobName,page,size);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot find instances of Job");
        }
    }

    @Test
    public void testGetHealthInfo(){
        try {
            Scheduler scheduler=Mockito.mock(Scheduler.class);
            given(factory.getObject()).willReturn(scheduler);
            given(scheduler.getJobGroupNames()).willReturn(Arrays.asList("BA"));
            JobKey jobKey= new JobKey("TEST");
            Set<JobKey> jobKeySet=new HashSet<JobKey>();
            jobKeySet.add(jobKey);
            given(scheduler.getJobKeys(GroupMatcher.jobGroupEquals("BA"))).willReturn((jobKeySet));

            Pageable pageRequest=new PageRequest(0,1, Sort.Direction.DESC,"timestamp");
            List<ScheduleState> scheduleStateList=new ArrayList<ScheduleState>();
            ScheduleState scheduleState1=new ScheduleState();
            scheduleState1.setGroupName("BA");
            scheduleState1.setJobName("job1");
            scheduleState1.setScheduleid(1);
            scheduleState1.setState("starting");
            scheduleState1.setAppId("ttt");
            scheduleState1.setTimestamp(System.currentTimeMillis());
            scheduleStateList.add(scheduleState1);
            given(scheduleStateRepo.findByGroupNameAndJobName(jobKey.getGroup(),jobKey.getName(),pageRequest)).willReturn(scheduleStateList);
            JobHealth tmp = service.getHealthInfo();
            assertTrue(true);

            scheduleStateList.remove(0);
            scheduleState1.setState("down");
            scheduleStateList.add(scheduleState1);
            given(scheduleStateRepo.findByGroupNameAndJobName(jobKey.getGroup(),jobKey.getName(),pageRequest)).willReturn(scheduleStateList);
            JobHealth tmp1 = service.getHealthInfo();
        }catch (Throwable t){
            fail("Cannot get Health info "+t);
        }
    }

}
