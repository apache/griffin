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
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.JobRequestBody;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.apache.griffin.core.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static org.apache.griffin.core.measure.MeasureTestHelper.createATestMeasure;
import static org.apache.griffin.core.measure.MeasureTestHelper.createJobDetail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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

    @MockBean
    private Properties sparkJobProps;

    @MockBean
    private RestTemplate restTemplate;

    @Autowired
    private JobServiceImpl service;

    @MockBean
    private MeasureRepo measureRepo;


    @Before
    public void setup() {

    }

    @Test
    public void testGetAliveJobsForNormalRun() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        JobDetailImpl jobDetail = createJobDetail();
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobGroupNames()).willReturn(Arrays.asList("group"));
        HashSet<JobKey> set = new HashSet<>();
        set.add(new JobKey("name", "group"));
        given(scheduler.getJobKeys(GroupMatcher.anyGroup())).willReturn(set);
        List<Trigger> triggers = Arrays.asList(newTriggerInstance("name", "group", 3000));
        JobKey jobKey = set.iterator().next();
        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(triggers);
        given(scheduler.getJobDetail(jobKey)).willReturn(jobDetail);
        assertEquals(service.getAliveJobs().size(), 1);
    }

    @Test
    public void testGetAliveJobsForNoJobsWithTriggerEmpty() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobGroupNames()).willReturn(Arrays.asList("group"));
        HashSet<JobKey> set = new HashSet<>();
        set.add(new JobKey("name", "group"));
        given(scheduler.getJobKeys(GroupMatcher.jobGroupEquals("group"))).willReturn(set);
        JobKey jobKey = set.iterator().next();
        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(Arrays.asList());
        assertEquals(service.getAliveJobs().size(), 0);
    }

    @Test
    public void testGetAliveJobsForSchedulerException() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobGroupNames()).willReturn(Arrays.asList("group"));
        HashSet<JobKey> set = new HashSet<>();
        set.add(new JobKey("name", "group"));
        given(scheduler.getJobKeys(GroupMatcher.anyGroup())).willReturn(set);
        JobKey jobKey = set.iterator().next();
        GriffinException.GetJobsFailureException exception = getTriggersOfJobExpectException(scheduler, jobKey);
        assertTrue(exception != null);
    }

    @Test
    public void testAddJobForSuccess() throws Exception {
        JobRequestBody jobRequestBody = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH",
                String.valueOf(System.currentTimeMillis()), String.valueOf(System.currentTimeMillis()), "1000");
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(measureRepo.findOne(1L)).willReturn(createATestMeasure("measureName","org"));
        assertEquals(service.addJob("BA", "jobName", 1L, jobRequestBody), GriffinOperationMessage.CREATE_JOB_SUCCESS);
    }

    @Test
    public void testAddJobForFailWithFormatError() {
        JobRequestBody jobRequestBody = new JobRequestBody();
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        assertEquals(service.addJob("BA", "jobName", 0L, jobRequestBody), GriffinOperationMessage.CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailWithTriggerKeyExist() throws SchedulerException {
        String groupName = "BA";
        String jobName = "jobName";
        JobRequestBody jobRequestBody = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH",
                String.valueOf(System.currentTimeMillis()), String.valueOf(System.currentTimeMillis()), "1000");
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(TriggerKey.triggerKey(jobName, groupName))).willReturn(true);
        assertEquals(service.addJob(groupName, jobName, 0L, jobRequestBody), GriffinOperationMessage.CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailWithScheduleException() throws SchedulerException {
        String groupName = "BA";
        String jobName = "jobName";
        JobRequestBody jobRequestBody = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH",
                String.valueOf(System.currentTimeMillis()), String.valueOf(System.currentTimeMillis()), "1000");
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        Trigger trigger = newTrigger().withIdentity(TriggerKey.triggerKey(jobName, groupName)).build();
        given(scheduler.scheduleJob(trigger)).willThrow(SchedulerException.class);
        assertEquals(service.addJob(groupName, jobName, 0L, jobRequestBody), GriffinOperationMessage.CREATE_JOB_FAIL);
    }

    @Test
    public void testDeleteJobForSuccess() throws SchedulerException {
        String groupName = "BA";
        String jobName = "jobName";
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobDetail(new JobKey(jobName, groupName))).willReturn(createJobDetail());
        assertEquals(service.deleteJob(groupName, jobName), GriffinOperationMessage.DELETE_JOB_SUCCESS);
    }

    @Test
    public void testDeleteJobForFailWithPauseFailure() throws SchedulerException {
        String groupName = "BA";
        String jobName = "jobName";
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(new JobKey(jobName, groupName));
        assertEquals(service.deleteJob(groupName, jobName), GriffinOperationMessage.DELETE_JOB_FAIL);
    }

    @Test
    public void testDeleteJobForFailWithNull() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        assertEquals(service.deleteJob("BA", "jobName"), GriffinOperationMessage.DELETE_JOB_FAIL);
    }

    @Test
    public void testFindInstancesOfJob() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        String groupName = "BA";
        String jobName = "job1";
        int page = 0;
        int size = 2;
        JobKey jobKey = new JobKey(jobName,groupName);
        JobInstance jobInstance = new JobInstance(groupName, jobName, 1, LivySessionStates.State.dead, "app_id", "app_uri", System.currentTimeMillis());
        Pageable pageRequest = new PageRequest(page, size, Sort.Direction.DESC, "timestamp");
        given(jobInstanceRepo.findByGroupNameAndJobName(groupName, jobName, pageRequest)).willReturn(Arrays.asList(jobInstance));
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(jobKey)).willReturn(true);
        mockJsonDataMap(scheduler, jobKey,false);
        assertEquals(service.findInstancesOfJob(groupName, jobName, page, size).size(), 1);
    }

    @Test
    public void testFindInstancesOfJobForDeleted() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        String groupName = "BA";
        String jobName = "job1";
        int page = 0;
        int size = 2;
        JobKey jobKey = new JobKey(jobName,groupName);
        JobInstance jobInstance = new JobInstance(groupName, jobName, 1, LivySessionStates.State.dead, "app_id", "app_uri", System.currentTimeMillis());
        Pageable pageRequest = new PageRequest(page, size, Sort.Direction.DESC, "timestamp");
        given(jobInstanceRepo.findByGroupNameAndJobName(groupName, jobName, pageRequest)).willReturn(Arrays.asList(jobInstance));
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(jobKey)).willReturn(true);
        mockJsonDataMap(scheduler, jobKey,true);
        assertEquals(service.findInstancesOfJob(groupName, jobName, page, size).size(), 0);
    }

    @Test
    public void testSyncInstancesOfJobForSuccess() {
        JobInstance instance = newJobInstance();
        String group = "groupName";
        String jobName = "jobName";
        given(jobInstanceRepo.findGroupWithJobName()).willReturn(Arrays.asList((Object) (new Object[]{group, jobName})));
        given(jobInstanceRepo.findByGroupNameAndJobName(group, jobName)).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        String result = "{\"id\":1,\"state\":\"starting\",\"appId\":123,\"appInfo\":{\"driverLogUrl\":null,\"sparkUiUrl\":null},\"log\":[]}";
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn(result);
        service.syncInstancesOfAllJobs();
    }


    @Test
    public void testSyncInstancesOfJobForRestClientException() {
        JobInstance instance = newJobInstance();
        instance.setSessionId(1234564);
        String group = "groupName";
        String jobName = "jobName";
        given(jobInstanceRepo.findGroupWithJobName()).willReturn(Arrays.asList((Object) (new Object[]{group, jobName})));
        given(jobInstanceRepo.findByGroupNameAndJobName(group, jobName)).willReturn(Arrays.asList(instance));
        given(sparkJobProps.getProperty("livy.uri")).willReturn(PropertiesUtil.getProperties("/sparkJob.properties").getProperty("livy.uri"));
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testSyncInstancesOfJobForIOException() throws Exception {
        JobInstance instance = newJobInstance();
        String group = "groupName";
        String jobName = "jobName";
        given(jobInstanceRepo.findGroupWithJobName()).willReturn(Arrays.asList((Object) (new Object[]{group, jobName})));
        given(jobInstanceRepo.findByGroupNameAndJobName(group, jobName)).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn("result");
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testSyncInstancesOfJobForIllegalArgumentException() throws Exception {
        JobInstance instance = newJobInstance();
        String group = "groupName";
        String jobName = "jobName";
        given(jobInstanceRepo.findGroupWithJobName()).willReturn(Arrays.asList((Object) (new Object[]{group, jobName})));
        given(jobInstanceRepo.findByGroupNameAndJobName(group, jobName)).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn("{\"state\":\"wrong\"}");
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testGetHealthInfoWithHealthy() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobGroupNames()).willReturn(Arrays.asList("BA"));
        JobKey jobKey = new JobKey("test");
        SimpleTrigger trigger = new SimpleTriggerImpl();
        List<Trigger> triggers = new ArrayList<>();
        triggers.add(trigger);
        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(triggers);
        mockJsonDataMap(scheduler, jobKey, false);
        Set<JobKey> jobKeySet = new HashSet<>();
        jobKeySet.add(jobKey);
        given(scheduler.getJobKeys(GroupMatcher.anyGroup())).willReturn((jobKeySet));

        Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "timestamp");
        List<JobInstance> scheduleStateList = new ArrayList<>();
        scheduleStateList.add(newJobInstance());
        given(jobInstanceRepo.findByGroupNameAndJobName(jobKey.getGroup(), jobKey.getName(), pageRequest)).willReturn(scheduleStateList);
        assertEquals(service.getHealthInfo().getHealthyJobCount(), 1);

    }

    @Test
    public void testGetHealthInfoWithUnhealthy() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.getJobGroupNames()).willReturn(Arrays.asList("BA"));
        JobKey jobKey = new JobKey("test");
        Set<JobKey> jobKeySet = new HashSet<>();
        jobKeySet.add(jobKey);
        given(scheduler.getJobKeys(GroupMatcher.jobGroupEquals("BA"))).willReturn((jobKeySet));

        Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "timestamp");
        List<JobInstance> scheduleStateList = new ArrayList<>();
        JobInstance jobInstance = newJobInstance();
        jobInstance.setState(LivySessionStates.State.error);
        scheduleStateList.add(jobInstance);
        given(jobInstanceRepo.findByGroupNameAndJobName(jobKey.getGroup(), jobKey.getName(), pageRequest)).willReturn(scheduleStateList);
        assertEquals(service.getHealthInfo().getHealthyJobCount(), 0);
    }

    private void mockJsonDataMap(Scheduler scheduler,JobKey jobKey,Boolean deleted) throws SchedulerException {
        JobDataMap jobDataMap = mock(JobDataMap.class);
        JobDetailImpl jobDetail = new JobDetailImpl();
        jobDetail.setJobDataMap(jobDataMap);
        given(scheduler.getJobDetail(jobKey)).willReturn(jobDetail);
        given(jobDataMap.getBooleanFromString("deleted")).willReturn(deleted);
    }

    private Trigger newTriggerInstance(String name, String group, int internalInSeconds) {
        return newTrigger().withIdentity(TriggerKey.triggerKey(name, group)).
                withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(internalInSeconds)
                        .repeatForever()).startAt(new Date()).build();
    }


    private GriffinException.GetJobsFailureException getTriggersOfJobExpectException(Scheduler scheduler, JobKey jobKey) {
        GriffinException.GetJobsFailureException exception = null;
        try {
            given(scheduler.getTriggersOfJob(jobKey)).willThrow(new GriffinException.GetJobsFailureException());
            service.getAliveJobs();
        } catch (GriffinException.GetJobsFailureException e) {
            exception = e;
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
        return exception;
    }

    private JobInstance newJobInstance() {
        JobInstance jobInstance = new JobInstance();
        jobInstance.setGroupName("BA");
        jobInstance.setJobName("job1");
        jobInstance.setSessionId(1);
        jobInstance.setState(LivySessionStates.State.starting);
        jobInstance.setAppId("app_id");
        jobInstance.setTimestamp(System.currentTimeMillis());
        return jobInstance;
    }
}
