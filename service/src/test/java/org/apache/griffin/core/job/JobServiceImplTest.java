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
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.job.repo.GriffinJobRepo;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.job.repo.JobScheduleRepo;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.repo.GriffinMeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.quartz.*;
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
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.griffin.core.util.EntityHelper.*;
import static org.apache.griffin.core.util.GriffinOperationMessage.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.isNotNull;
import static org.mockito.Mockito.doThrow;

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
    private GriffinMeasureRepo griffinMeasureRepo;

    @MockBean
    private GriffinJobRepo jobRepo;

    @MockBean
    private JobInstanceRepo jobInstanceRepo;

    @MockBean
    private SchedulerFactoryBean factory;

    @MockBean(name = "livyConf")
    private Properties sparkJobProps;

    @MockBean
    private RestTemplate restTemplate;

    @Autowired
    private JobServiceImpl service;


    @Before
    public void setup() {

    }

    @Test
    public void testGetAliveJobsForSuccess() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        SimpleTrigger trigger = new SimpleTriggerImpl();
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(Arrays.asList(trigger));
        assertEquals(service.getAliveJobs().size(), 1);
    }

    @Test
    public void testGetAliveJobsForNoJobsWithTriggerEmpty() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(new ArrayList<>());
        assertEquals(service.getAliveJobs().size(), 0);
    }

    @Test
    public void testGetAliveJobsForNoJobsWithException() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        GriffinException.GetJobsFailureException exception = getExceptionForGetAliveJObs(scheduler);
        assert exception != null;
    }


    @Test
    public void testAddJobForSuccess() throws Exception {
        JobSchedule js = createJobSchedule();
        js.setId(1L);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        given(jobRepo.countByJobNameAndDeleted(js.getJobName(), false)).willReturn(0);
        given(jobScheduleRepo.save(js)).willReturn(js);
        given(jobRepo.save(Matchers.any(GriffinJob.class))).willReturn(job);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_SUCCESS);
    }

    @Test
    public void testAddJobForFailureWithMeasureNull() throws Exception {
        JobSchedule js = createJobSchedule();
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(null);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailureWitJobNameRepeat() throws Exception {
        JobSchedule js = createJobSchedule();
        GriffinMeasure measure = createGriffinMeasure("measureName");
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        given(jobRepo.countByJobNameAndDeleted(js.getJobName(), false)).willReturn(1);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailureWitJobNameNull() throws Exception {
        JobSchedule js = createJobSchedule(null);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailureWithBaselineInvalid() throws Exception {
        JobDataSegment source = createJobDataSegment("source_name", false);
        JobDataSegment target = createJobDataSegment("target_name", false);
        JobSchedule js = createJobSchedule("jobName", source, target);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailureWithConnectorNameInvalid() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDataSegment source = createJobDataSegment("source_connector_name", true);
        JobDataSegment target = createJobDataSegment("target_name", false);
        JobSchedule js = createJobSchedule("jobName", source, target);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailureWithMeasureConnectorNameRepeat() throws Exception {
        JobSchedule js = createJobSchedule();
        DataConnector dcSource = createDataConnector("connector_name", "default", "test_data_src", "dt=#YYYYMMdd# AND hour=#HH#");
        DataConnector dcTarget = createDataConnector("connector_name", "default", "test_data_tgt", "dt=#YYYYMMdd# AND hour=#HH#");
        GriffinMeasure measure = createGriffinMeasure("measureName", dcSource, dcTarget);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailureWithJobScheduleConnectorNameRepeat() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDataSegment source = createJobDataSegment("source_name", true);
        JobDataSegment target = createJobDataSegment("source_name", false);
        JobSchedule js = createJobSchedule("jobName", source, target);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_FAIL);
    }

    @Test
    public void testAddJobForFailureWithTriggerKeyExist() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDataSegment source = createJobDataSegment("source_name", true);
        JobDataSegment target = createJobDataSegment("target_name", false);
        JobSchedule js = createJobSchedule("jobName", source, target);
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        given(scheduler.checkExists(Matchers.any(TriggerKey.class))).willReturn(true);
        GriffinOperationMessage message = service.addJob(js);
        assertEquals(message, CREATE_JOB_FAIL);
    }

    @Test
    public void testDeleteJobByIdForSuccessWithTriggerKeyExist() throws SchedulerException {
        Long jobId = 1L;
        GriffinJob job = new GriffinJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        assertEquals(service.deleteJob(jobId), DELETE_JOB_SUCCESS);
    }

    @Test
    public void testDeleteJobByIdForSuccessWithTriggerKeyNotExist() throws SchedulerException {
        Long jobId = 1L;
        GriffinJob job = new GriffinJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);
        assertEquals(service.deleteJob(jobId), DELETE_JOB_SUCCESS);
    }

    @Test
    public void testDeleteJobByIdForFailureWithNull() throws SchedulerException {
        Long jobId = 1L;
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(null);
        assertEquals(service.deleteJob(jobId), DELETE_JOB_FAIL);
    }

    @Test
    public void testDeleteJobByIdForFailureWithException() throws SchedulerException {
        Long jobId = 1L;
        GriffinJob job = new GriffinJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(Matchers.any(JobKey.class));
        assertEquals(service.deleteJob(jobId), DELETE_JOB_FAIL);
    }


    @Test
    public void testDeleteJobByNameForSuccessWithTriggerKeyExist() throws SchedulerException {
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(jobRepo.findByJobNameAndDeleted(job.getJobName(), false)).willReturn(Arrays.asList(job));
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        assertEquals(service.deleteJob(job.getJobName()), DELETE_JOB_SUCCESS);
    }

    @Test
    public void testDeleteJobByNameForSuccessWithTriggerKeyNotExist() throws SchedulerException {
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(jobRepo.findByJobNameAndDeleted(job.getJobName(), false)).willReturn(Arrays.asList(job));
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);
        assertEquals(service.deleteJob(job.getJobName()), DELETE_JOB_SUCCESS);
    }

    @Test
    public void testDeleteJobByJobNameForFailureWithNull() throws SchedulerException {
        String jobName = "jobName";
        given(jobRepo.findByJobNameAndDeleted(jobName, false)).willReturn(new ArrayList<>());
        assertEquals(service.deleteJob(jobName), DELETE_JOB_FAIL);
    }

    @Test
    public void testDeleteJobByJobNameForFailureWithException() throws SchedulerException {
        Long jobId = 1L;
        GriffinJob job = new GriffinJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByJobNameAndDeleted(job.getJobName(), false)).willReturn(Arrays.asList(job));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(Matchers.any(JobKey.class));
        assertEquals(service.deleteJob(jobId), DELETE_JOB_FAIL);
    }

    @Test
    public void testDeleteJobsRelateToMeasureForSuccessWithTriggerKeyExist() throws SchedulerException {
        Long jobId = 1L;
        Long measureId = 1L;
        GriffinJob job = new GriffinJob(measureId, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(jobRepo.findByMeasureIdAndDeleted(measureId, false)).willReturn(Arrays.asList(job));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        assertEquals(service.deleteJobsRelateToMeasure(measureId), true);
    }

    @Test
    public void testDeleteJobsRelateToMeasureForSuccessWithTriggerKeyNotExist() throws SchedulerException {
        Long jobId = 1L;
        Long measureId = 1L;
        GriffinJob job = new GriffinJob(measureId, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(jobRepo.findByMeasureIdAndDeleted(measureId, false)).willReturn(Arrays.asList(job));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);
        assertEquals(service.deleteJobsRelateToMeasure(measureId), true);
    }

    @Test
    public void testDeleteJobsRelateToMeasureForSuccessWithNull() throws SchedulerException {
        Long measureId = 1L;
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByMeasureIdAndDeleted(measureId, false)).willReturn(null);
        assertEquals(service.deleteJobsRelateToMeasure(measureId), true);
    }

    @Test
    public void testDeleteJobsRelateToMeasureForFailureWithException() throws SchedulerException {
        Long jobId = 1L;
        Long measureId = 1L;
        GriffinJob job = new GriffinJob(measureId, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(jobRepo.findByMeasureIdAndDeleted(measureId, false)).willReturn(Arrays.asList(job));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(Matchers.any(JobKey.class));
        assertEquals(service.deleteJobsRelateToMeasure(measureId), false);
    }

    @Test
    public void testFindInstancesOfJobForSuccess() throws SchedulerException {
        Long jobId = 1L;
        int page = 0;
        int size = 2;
        GriffinJob job = new GriffinJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean jobInstance = new JobInstanceBean(1L, LivySessionStates.State.dead, "app_id", "app_uri", null, null);
        Pageable pageRequest = new PageRequest(page, size, Sort.Direction.DESC, "tms");
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(jobInstanceRepo.findByJobId(1L, pageRequest)).willReturn(Arrays.asList(jobInstance));
        assertEquals(service.findInstancesOfJob(1L, page, size).size(), 1);
    }

    @Test
    public void testFindInstancesOfJobWithNull() throws SchedulerException {
        Long jobId = 1L;
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(null);
        assertEquals(service.findInstancesOfJob(jobId, 0, 2).size(), 0);
    }

    @Test
    public void testDeleteExpiredJobInstanceForSuccessWithTriggerKeyExist() throws SchedulerException {
        JobInstanceBean jobInstance = new JobInstanceBean(LivySessionStates.State.dead, "pName", "pGroup", null, null);
        given(jobInstanceRepo.findByExpireTmsLessThanEqual(Matchers.any())).willReturn(Arrays.asList(jobInstance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        service.deleteExpiredJobInstance();
    }

    @Test
    public void testDeleteExpiredJobInstanceForSuccessWithTriggerKeyNotExist() throws SchedulerException {
        JobInstanceBean jobInstance = new JobInstanceBean(LivySessionStates.State.dead, "pName", "pGroup", null, null);
        given(jobInstanceRepo.findByExpireTmsLessThanEqual(Matchers.any())).willReturn(Arrays.asList(jobInstance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);
        service.deleteExpiredJobInstance();
    }

    @Test
    public void testDeleteExpiredJobInstanceForSuccessWithNull() throws SchedulerException {
        given(jobInstanceRepo.findByExpireTmsLessThanEqual(Matchers.any())).willReturn(null);
        service.deleteExpiredJobInstance();
    }

    @Test
    public void testDeleteExpiredJobInstanceForFailureWithException() throws SchedulerException {
        JobInstanceBean jobInstance = new JobInstanceBean(LivySessionStates.State.dead, "pName", "pGroup", null, null);
        given(jobInstanceRepo.findByExpireTmsLessThanEqual(Matchers.any())).willReturn(Arrays.asList(jobInstance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getObject()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(Matchers.any(JobKey.class));
        service.deleteExpiredJobInstance();
    }

    @Test
    public void testSyncInstancesOfJobForSuccess() {
        JobInstanceBean instance = createJobInstance();
        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        String result = "{\"id\":1,\"state\":\"starting\",\"appId\":123,\"appInfo\":{\"driverLogUrl\":null,\"sparkUiUrl\":null},\"log\":[]}";
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn(result);
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testSyncInstancesOfJobForFailureWithRestClientException() {
        JobInstanceBean instance = createJobInstance();
        instance.setSessionId(1234564L);
        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willThrow(RestClientException.class);
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testSyncInstancesOfJobForFailureWithIOException() throws Exception {
        JobInstanceBean instance = createJobInstance();
        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn("result");
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testSyncInstancesOfJobForFailureWithIllegalArgumentException() throws Exception {
        JobInstanceBean instance = createJobInstance();
        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn("{\"state\":\"wrong\"}");
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testSyncInstancesOfJobForFailureWithException() throws Exception {
        JobInstanceBean instance = createJobInstance();
        given(jobInstanceRepo.findByActiveState()).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        String result = "{\"id\":1,\"state\":\"starting\",\"appId\":123,\"appInfo\":{\"driverLogUrl\":null,\"sparkUiUrl\":null},\"log\":[]}";
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn(result);
        doThrow(Exception.class).when(jobInstanceRepo).save(Matchers.any(JobInstanceBean.class));
        service.syncInstancesOfAllJobs();
    }

    @Test
    public void testGetHealthInfoWithHealthy() throws SchedulerException {
        Long jobId = 1L;
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        GriffinJob job = new GriffinJob(jobId, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        SimpleTrigger trigger = new SimpleTriggerImpl();
        List<Trigger> triggers = new ArrayList<>();
        triggers.add(trigger);
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(triggers);

        Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "tms");
        given(jobInstanceRepo.findByJobId(jobId, pageRequest)).willReturn(Arrays.asList(createJobInstance()));
        assertEquals(service.getHealthInfo().getHealthyJobCount(), 1);

    }

    @Test
    public void testGetHealthInfoWithUnhealthy() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        SimpleTrigger trigger = new SimpleTriggerImpl();
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(Arrays.asList(trigger));

        Pageable pageRequest = new PageRequest(0, 1, Sort.Direction.DESC, "tms");
        List<JobInstanceBean> scheduleStateList = new ArrayList<>();
        JobInstanceBean instance = createJobInstance();
        instance.setState(LivySessionStates.State.error);
        scheduleStateList.add(instance);
        given(jobInstanceRepo.findByJobId(1L, pageRequest)).willReturn(scheduleStateList);
        assertEquals(service.getHealthInfo().getHealthyJobCount(), 0);
    }

    @Test
    public void testGetHealthInfoWithException() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        GriffinJob job = new GriffinJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        given(factory.getObject()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        GriffinException.GetHealthInfoFailureException exception = getExceptionForHealthInfo(scheduler);
        assert exception != null;
    }


    private GriffinException.GetHealthInfoFailureException getExceptionForHealthInfo(Scheduler scheduler) throws SchedulerException {
        GriffinException.GetHealthInfoFailureException exception = null;
        try {
            given(scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willThrow(SchedulerException.class);
            service.getHealthInfo();
        } catch (GriffinException.GetHealthInfoFailureException e) {
            exception = e;
        }
        return exception;
    }

    private GriffinException.GetJobsFailureException getExceptionForGetAliveJObs(Scheduler scheduler) throws SchedulerException {
        GriffinException.GetJobsFailureException exception = null;
        try {
            given(scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willThrow(new GriffinException.GetJobsFailureException());
            service.getAliveJobs();
        } catch (GriffinException.GetJobsFailureException e) {
            exception = e;
        }
        return exception;
    }

}
