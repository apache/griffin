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

import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.JobRequestBody;
import org.apache.griffin.core.job.entity.LivySessionStateMap;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
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
public class JobServiceImplTest {
    private static final Logger log = LoggerFactory.getLogger(JobServiceImplTest.class);

    @TestConfiguration
    public static class SchedulerServiceConfiguration{
        @Bean
        public JobServiceImpl service(){
            return new JobServiceImpl();
        }
        @Bean
        public SchedulerFactoryBean factoryBean(){
            return new SchedulerFactoryBean();
        }
    }

    @MockBean
    private JobInstanceRepo jobInstanceRepo;


    @MockBean
    private SchedulerFactoryBean factory;

    @Autowired
    private JobServiceImpl service;

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
            JobRequestBody jobRequestBody =new JobRequestBody();
            Scheduler scheduler=Mockito.mock(Scheduler.class);
            given(factory.getObject()).willReturn(scheduler);
            GriffinOperationMessage tmp = service.addJob(groupName,jobName,measureName, jobRequestBody);
            assertEquals(tmp,GriffinOperationMessage.CREATE_JOB_FAIL);
            assertTrue(true);

            JobRequestBody jobRequestBody1 =new JobRequestBody("YYYYMMdd-HH","YYYYMMdd-HH",
                    System.currentTimeMillis()+"",System.currentTimeMillis()+"","1000");
            Scheduler scheduler1=Mockito.mock(Scheduler.class);
            given(factory.getObject()).willReturn(scheduler1);
            GriffinOperationMessage tmp1 = service.addJob(groupName,jobName,measureName, jobRequestBody1);
            assertEquals(tmp1,GriffinOperationMessage.CREATE_JOB_SUCCESS);
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
            GriffinOperationMessage tmp = service.deleteJob(groupName,jobName);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot delete job");
        }
        try {
            given(factory.getObject()).willThrow(SchedulerException.class);
            GriffinOperationMessage tmp = service.deleteJob(groupName,jobName);
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
            List<JobInstance> tmp = service.findInstancesOfJob(groupName,jobName,page,size);
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
            List<JobInstance> scheduleStateList=new ArrayList<JobInstance>();
            JobInstance jobInstance=new JobInstance();
            jobInstance.setGroupName("BA");
            jobInstance.setJobName("job1");
            jobInstance.setSessionId(1);
            jobInstance.setState(LivySessionStateMap.State.starting);
            jobInstance.setAppId("ttt");
            jobInstance.setTimestamp(System.currentTimeMillis());
            scheduleStateList.add(jobInstance);
            given(jobInstanceRepo.findByGroupNameAndJobName(jobKey.getGroup(),jobKey.getName(),pageRequest)).willReturn(scheduleStateList);
            JobHealth tmp = service.getHealthInfo();
            assertTrue(true);

            scheduleStateList.remove(0);
            jobInstance.setState(LivySessionStateMap.State.success);
            scheduleStateList.add(jobInstance);
            given(jobInstanceRepo.findByGroupNameAndJobName(jobKey.getGroup(),jobKey.getName(),pageRequest)).willReturn(scheduleStateList);
            JobHealth tmp1 = service.getHealthInfo();
        }catch (Throwable t){
            fail("Cannot get Health info "+t);
        }
    }

}
