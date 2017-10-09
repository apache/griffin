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

import org.apache.griffin.core.error.exception.GriffinException;
import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.JobRequestBody;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

import static org.apache.griffin.core.measure.MeasureTestHelper.createJobDetail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.quartz.TriggerBuilder.newTrigger;

@RunWith(SpringRunner.class)
public class JobServiceImplTest {

    @TestConfiguration
    public static class SchedulerServiceConfiguration {
        @Bean
        public JobServiceImpl service() {
            return new JobServiceImpl();
        }

        @Bean
        public SchedulerFactoryBean factoryBean() {
            return new SchedulerFactoryBean();
        }
    }

    @MockBean
    private JobInstanceRepo jobInstanceRepo;


    @MockBean
    private SchedulerFactoryBean factory;

    @Autowired
    public JobServiceImpl service;

    @Before
    public void setup() {
    }

    @Test
    public void testGetAliveJobs() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        JobDetailImpl jobDetail = createJobDetail();
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobGroupNames()).willReturn(Arrays.asList("group"));
        HashSet<JobKey> set = new HashSet<JobKey>() {{
            add(new JobKey("name", "group"));
        }};
        given(scheduler.getJobKeys(GroupMatcher.jobGroupEquals("group"))).willReturn(set);
        Trigger trigger = newTrigger().withIdentity(TriggerKey.triggerKey("name", "group")).
                withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(3000).repeatForever())
                .startAt(new Date()).build();
        List<Trigger> triggers = Arrays.asList(trigger);
        JobKey jobKey = set.iterator().next();
        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(triggers);
        given(scheduler.getJobDetail(jobKey)).willReturn(jobDetail);
        assertEquals(service.getAliveJobs().size(), 1);

        // trigger is empty
        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(Arrays.asList());
        assertEquals(service.getAliveJobs().size(), 0);

        // schedule exception
        GriffinException.GetJobsFailureException exception = null;
        try {
            given(scheduler.getTriggersOfJob(jobKey)).willThrow(new GriffinException.GetJobsFailureException());
            service.getAliveJobs();
        } catch (GriffinException.GetJobsFailureException e) {
            exception = e;
        }
        assertTrue(exception != null);


    }

    @Test
    public void testAddJob() {
        String groupName = "BA";
        String jobName = "job1";
        long measureId = 0;
        JobRequestBody jobRequestBody = new JobRequestBody();
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        assertEquals(service.addJob(groupName, jobName, measureId, jobRequestBody), GriffinOperationMessage.CREATE_JOB_FAIL);

        JobRequestBody jobRequestBody1 = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH",
                System.currentTimeMillis() + "", System.currentTimeMillis() + "", "1000");
        Scheduler scheduler1 = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler1);
        assertEquals(service.addJob(groupName, jobName, measureId, jobRequestBody1), GriffinOperationMessage.CREATE_JOB_SUCCESS);
    }

    @Test
    public void testDeleteJob() throws SchedulerException {
        String groupName = "BA";
        String jobName = "job1";
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        // DELETE_JOB_SUCCESS
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobDetail(new JobKey(jobName,groupName))).willReturn(createJobDetail());
        assertEquals(service.deleteJob(groupName, jobName), GriffinOperationMessage.DELETE_JOB_SUCCESS);

        // DELETE_JOB_FAIL
        given(factory.getObject()).willThrow(SchedulerException.class);
        assertEquals(service.deleteJob(groupName, jobName), GriffinOperationMessage.DELETE_JOB_FAIL);
    }

    @Test
    public void testFindInstancesOfJob() {
        String groupName = "BA";
        String jobName = "job1";
        int page = 0;
        int size = 2;
        JobInstance jobInstance = new JobInstance(groupName, jobName, 1, LivySessionStates.State.dead, "app_id", "app_uri", System.currentTimeMillis());
        Pageable pageRequest = new PageRequest(page, size, Sort.Direction.DESC, "timestamp");
        given(jobInstanceRepo.findByGroupNameAndJobName(groupName, jobName, pageRequest)).willReturn(Arrays.asList(jobInstance));
        assertEquals(service.findInstancesOfJob(groupName, jobName, page, size).size(),1);
    }

    @Test
    public void testGetHealthInfo() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobGroupNames()).willReturn(Arrays.asList("BA"));
        JobKey jobKey = new JobKey("test");
        Set<JobKey> jobKeySet = new HashSet<>();
        jobKeySet.add(jobKey);
        given(scheduler.getJobKeys(GroupMatcher.jobGroupEquals("BA"))).willReturn((jobKeySet));

        Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "timestamp");
        List<JobInstance> scheduleStateList = new ArrayList<>();
        JobInstance jobInstance = new JobInstance();
        jobInstance.setGroupName("BA");
        jobInstance.setJobName("job1");
        jobInstance.setSessionId(1);
        jobInstance.setState(LivySessionStates.State.starting);
        jobInstance.setAppId("app_id");
        jobInstance.setTimestamp(System.currentTimeMillis());
        scheduleStateList.add(jobInstance);
        given(jobInstanceRepo.findByGroupNameAndJobName(jobKey.getGroup(), jobKey.getName(), pageRequest)).willReturn(scheduleStateList);
        JobHealth health1 = service.getHealthInfo();
        assertEquals(health1.getHealthyJobCount(),1);

        scheduleStateList.remove(0);
        jobInstance.setState(LivySessionStates.State.error);
        scheduleStateList.add(jobInstance);
        given(jobInstanceRepo.findByGroupNameAndJobName(jobKey.getGroup(), jobKey.getName(), pageRequest)).willReturn(scheduleStateList);
        JobHealth health2 = service.getHealthInfo();
        assertEquals(health2.getHealthyJobCount(),0);
    }


}
