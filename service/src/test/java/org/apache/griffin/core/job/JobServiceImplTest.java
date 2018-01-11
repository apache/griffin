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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.griffin.core.error.exception.GriffinException;
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.job.repo.JobRepo;
import org.apache.griffin.core.job.repo.JobScheduleRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.apache.griffin.core.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
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
    private JobScheduleRepo jobScheduleRepo;

    @MockBean
    private MeasureRepo measureRepo;

    @MockBean
    private JobRepo<GriffinJob> jobRepo;
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


    @Before
    public void setup() {

    }

    @Test
    public void testGetAliveJobsForNormalRun() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        JobKey jobKey = new JobKey(job.getQuartzName(), job.getQuartzGroup());
        SimpleTrigger trigger = new SimpleTriggerImpl();
        List<Trigger> triggers = new ArrayList<>();
        triggers.add(trigger);
        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(triggers);
        assertEquals(service.getAliveJobs().size(), 1);
    }

    @Test
    public void testGetAliveJobsForNoJobsWithTriggerEmpty() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        JobKey jobKey = new JobKey(job.getQuartzName(), job.getQuartzGroup());
        List<Trigger> triggers = new ArrayList<>();
        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(triggers);
        assertEquals(service.getAliveJobs().size(), 0);
    }


//    @Test
//    public void testAddJobForSuccess() throws Exception {
//        JobSchedule js = createJobSchedule();
//
//        JobRequestBody jobRequestBody = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH",
//                String.valueOf(System.currentTimeMillis()), String.valueOf(System.currentTimeMillis()), "1000");
//        Scheduler scheduler = Mockito.mock(Scheduler.class);
//        given(factory.getObject()).willReturn(scheduler);
//        given(measureRepo.findOne(1L)).willReturn(createATestGriffinMeasure("measureName","org"));
//        assertEquals(service.addJob("BA", "jobName", 1L, jobRequestBody), GriffinOperationMessage.CREATE_JOB_SUCCESS);
//    }
//
//    @Test
//    public void testAddJobForFailWithFormatError() {
//        JobRequestBody jobRequestBody = new JobRequestBody();
//        Scheduler scheduler = Mockito.mock(Scheduler.class);
//        given(factory.getObject()).willReturn(scheduler);
//        assertEquals(service.addJob("BA", "jobName", 0L, jobRequestBody), GriffinOperationMessage.CREATE_JOB_FAIL);
//    }
//
//    @Test
//    public void testAddJobForFailWithTriggerKeyExist() throws SchedulerException {
//        String groupName = "BA";
//        String jobName = "jobName";
//        JobRequestBody jobRequestBody = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH",
//                String.valueOf(System.currentTimeMillis()), String.valueOf(System.currentTimeMillis()), "1000");
//        Scheduler scheduler = Mockito.mock(Scheduler.class);
//        given(factory.getObject()).willReturn(scheduler);
//        given(scheduler.checkExists(TriggerKey.triggerKey(jobName, groupName))).willReturn(true);
//        assertEquals(service.addJob(groupName, jobName, 0L, jobRequestBody), GriffinOperationMessage.CREATE_JOB_FAIL);
//    }
//
//    @Test
//    public void testAddJobForFailWithScheduleException() throws SchedulerException {
//        String groupName = "BA";
//        String jobName = "jobName";
//        JobRequestBody jobRequestBody = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH",
//                String.valueOf(System.currentTimeMillis()), String.valueOf(System.currentTimeMillis()), "1000");
//        Scheduler scheduler = Mockito.mock(Scheduler.class);
//        given(factory.getObject()).willReturn(scheduler);
//        Trigger trigger = newTrigger().withIdentity(TriggerKey.triggerKey(jobName, groupName)).build();
//        given(scheduler.scheduleJob(trigger)).willThrow(SchedulerException.class);
//        assertEquals(service.addJob(groupName, jobName, 0L, jobRequestBody), GriffinOperationMessage.CREATE_JOB_FAIL);
//    }

    @Test
    public void testDeleteJobForJobIdSuccess() throws SchedulerException {
        Long jobId = 1L;
//        GriffinJob job = new GriffinJob(1L, "jobName", "quartzJobName", "quartzGroupName", "pJobName", "pGroupName", false);
//        Scheduler scheduler = Mockito.mock(Scheduler.class);
//        JobKey jobKey = new JobKey(job.getQuartzJobName(), job.getQuartzGroupName());
//        JobKey pJobKey = new JobKey(job.getJobName(), job.getGroupName());
//        given(factory.getObject()).willReturn(scheduler);
//        given(scheduler.checkExists(pJobKey)).willReturn(true);
//        given(scheduler.checkExists(jobKey)).willReturn(true);
//        doNothing().when(scheduler).pauseJob(pJobKey);
//        doNothing().when(scheduler).pauseJob(jobKey);
//        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
//        assertEquals(service.deleteJob(jobId), GriffinOperationMessage.DELETE_JOB_SUCCESS);
    }

    @Test
    public void testDeleteJobForJobIdFailureWithNull() throws SchedulerException {
        Long jobId = 1L;
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(null);
        assertEquals(service.deleteJob(jobId), GriffinOperationMessage.DELETE_JOB_FAIL);
    }

    @Test
    public void testDeleteJobForJobIdFailureWithTriggerNotExist() throws SchedulerException {
        Long jobId = 1L;
        GriffinJob job = new GriffinJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        JobKey jobKey = new JobKey(job.getQuartzName(), job.getQuartzGroup());
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(jobKey)).willReturn(false);
        assertEquals(service.deleteJob(jobId), GriffinOperationMessage.DELETE_JOB_FAIL);
    }


    @Test
    public void testDeleteJobForJobNameSuccess() throws SchedulerException {
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        JobKey jobKey = new JobKey(job.getQuartzName(), job.getQuartzGroup());
//        given(jobRepo.findByJobNameAndDeleted(job.getJobName(), false)).willReturn(Arrays.asList(job));
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(jobKey)).willReturn(true);
        doNothing().when(scheduler).pauseJob(jobKey);
        assertEquals(service.deleteJob(job.getJobName()), GriffinOperationMessage.DELETE_JOB_SUCCESS);
    }

    @Test
    public void testDeleteJobForJobNameFailureWithNull() throws SchedulerException {
        String jobName = "jobName";
//        given(jobRepo.findByJobNameAndDeleted(jobName, false)).willReturn(new ArrayList<>());
        assertEquals(service.deleteJob(jobName), GriffinOperationMessage.DELETE_JOB_FAIL);
    }

    @Test
    public void testDeleteJobForJobNameFailureWithTriggerNotExist() throws SchedulerException {
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        JobKey jobKey = new JobKey(job.getQuartzName(), job.getQuartzGroup());
//        given(jobRepo.findByJobNameAndDeleted(job.getJobName(), false)).willReturn(Arrays.asList(job));
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(jobKey)).willReturn(false);
        assertEquals(service.deleteJob(job.getJobName()), GriffinOperationMessage.DELETE_JOB_FAIL);
    }

//    @Test
//    public void testFindInstancesOfJobForSuccess() throws SchedulerException {
//        Long jobId = 1L;
//        int page = 0;
//        int size = 2;
//        GriffinJob job = new GriffinJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
//        JobInstanceBean jobInstance = new JobInstanceBean(1L, LivySessionStates.State.dead, "app_id", "app_uri", System.currentTimeMillis(), System.currentTimeMillis());
//        Pageable pageRequest = new PageRequest(page, size, Sort.Direction.DESC, "timestamp");
//        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
//        given(jobInstanceRepo.findByJobId(1L, pageRequest)).willReturn(Arrays.asList(jobInstance));
//        assertEquals(service.findInstancesOfJob(1L, page, size).size(), 1);
//    }
//
//    @Test
//    public void testFindInstancesOfJobForNull() throws SchedulerException {
//        Long jobId = 1L;
//        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(null);
//        assertEquals(service.findInstancesOfJob(jobId, 0, 2).size(), 0);
//    }
//
//    @Test
//    public void testSyncInstancesOfJobForSuccess() {
//        JobInstanceBean instance = createJobInstance();
//        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
//        Whitebox.setInternalState(service, "restTemplate", restTemplate);
//        String result = "{\"id\":1,\"state\":\"starting\",\"appId\":123,\"appInfo\":{\"driverLogUrl\":null,\"sparkUiUrl\":null},\"log\":[]}";
//        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn(result);
//        service.syncInstancesOfAllJobs();
//    }

    @Test
    public void testSyncInstancesOfJobForRestClientException() {
        JobInstanceBean instance = createJobInstance();
        instance.setSessionId(1234564L);
        String path = "/sparkJob.properties";
        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
        given(sparkJobProps.getProperty("livy.uri")).willReturn(PropertiesUtil.getProperties(path, new ClassPathResource(path)).getProperty("livy.uri"));
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testSyncInstancesOfJobForIOException() throws Exception {
        JobInstanceBean instance = createJobInstance();
        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn("result");
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testSyncInstancesOfJobForIllegalArgumentException() throws Exception {
        JobInstanceBean instance = createJobInstance();
        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn("{\"state\":\"wrong\"}");
        service.syncInstancesOfAllJobs();
    }

//    @Test
//    public void testGetHealthInfoWithHealthy() throws SchedulerException {
//        Scheduler scheduler = Mockito.mock(Scheduler.class);
//        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
//        given(factory.getObject()).willReturn(scheduler);
//        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
//        JobKey jobKey = new JobKey(job.getQuartzJobName(), job.getQuartzGroupName());
//        SimpleTrigger trigger = new SimpleTriggerImpl();
//        List<Trigger> triggers = new ArrayList<>();
//        triggers.add(trigger);
//        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(triggers);
//
//        Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "timestamp");
//        List<JobInstanceBean> scheduleStateList = new ArrayList<>();
//        scheduleStateList.add(createJobInstance());
//        given(jobInstanceRepo.findByJobId(1L, pageRequest)).willReturn(scheduleStateList);
//        assertEquals(service.getHealthInfo().getHealthyJobCount(), 1);
//
//    }
//
//    @Test
//    public void testGetHealthInfoWithUnhealthy() throws SchedulerException {
//        Scheduler scheduler = Mockito.mock(Scheduler.class);
//        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
//        given(factory.getObject()).willReturn(scheduler);
//        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
//        JobKey jobKey = new JobKey(job.getQuartzJobName(), job.getQuartzGroupName());
//        SimpleTrigger trigger = new SimpleTriggerImpl();
//        List<Trigger> triggers = new ArrayList<>();
//        triggers.add(trigger);
//        given((List<Trigger>) scheduler.getTriggersOfJob(jobKey)).willReturn(triggers);
//
//        Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "timestamp");
//        List<JobInstanceBean> scheduleStateList = new ArrayList<>();
//        JobInstanceBean instance = createJobInstance();
//        instance.setState(LivySessionStates.State.error);
//        scheduleStateList.add(instance);
//        given(jobInstanceRepo.findByJobId(1L, pageRequest)).willReturn(scheduleStateList);
//        assertEquals(service.getHealthInfo().getHealthyJobCount(), 0);
//    }

    private void mockJsonDataMap(Scheduler scheduler, JobKey jobKey, Boolean deleted) throws SchedulerException {
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

    private JobInstanceBean createJobInstance() {
        JobInstanceBean jobBean = new JobInstanceBean();
        jobBean.setSessionId(1L);
        jobBean.setState(LivySessionStates.State.starting);
        jobBean.setAppId("app_id");
        jobBean.setTms(System.currentTimeMillis());
        return jobBean;
    }

    private JobSchedule createJobSchedule() throws JsonProcessingException {
        JobDataSegment segment = new JobDataSegment("data_connector_name", true);
        List<JobDataSegment> segments = Arrays.asList(segment);
        return new JobSchedule(1L,"jobName","0 0/4 * * * ?","GMT+8:00",segments);
    }
}
