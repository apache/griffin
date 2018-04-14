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

import org.apache.griffin.core.exception.GriffinException;
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.job.repo.*;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.repo.GriffinMeasureRepo;
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
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static org.apache.griffin.core.job.entity.LivySessionStates.State;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.*;
import static org.apache.griffin.core.util.EntityHelper.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
public class JobServiceImplTest {

    @TestConfiguration
    public static class SchedulerServiceConfiguration {
        @Bean("jobServiceImpl")
        public JobServiceImpl service() {
            return new JobServiceImpl();
        }

        @Bean(name = "schedulerFactoryBean")
        public SchedulerFactoryBean factoryBean() {
            return new SchedulerFactoryBean();
        }
    }

    @MockBean
    private JobScheduleRepo jobScheduleRepo;

    @MockBean
    private GriffinMeasureRepo griffinMeasureRepo;

    @MockBean
    private BatchJobRepo jobRepo;

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

    @MockBean
    private JobRepo<AbstractJob> repo;

    @MockBean
    private StreamingJobRepo streamingJobRepo;
    @MockBean
    private BatchJobOperatorImpl batchJobOp;
    @MockBean
    private StreamingJobOperatorImpl streamingJobOp;


    @Before
    public void setup() {

    }

    @Test
    public void testGetAliveJobsForSuccess() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        BatchJob job = createGriffinJob();
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        SimpleTrigger trigger = new SimpleTriggerImpl();
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(Arrays.asList(trigger));

        assertEquals(service.getAliveJobs("batch").size(), 1);
    }

    @Test
    public void testGetAliveJobsForNoJobsWithTriggerEmpty() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        BatchJob job = createGriffinJob();
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(new ArrayList<>());

        assertEquals(service.getAliveJobs("batch").size(), 0);
    }

    @Test(expected = GriffinException.ServiceException.class)
    public void testGetAliveJobsForNoJobsWithException() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        BatchJob job = createGriffinJob();
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        given(scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willThrow(new SchedulerException());

        service.getAliveJobs("batch");
    }


    @Test
    public void testAddJobForSuccess() throws Exception {
        JobSchedule js = createJobSchedule();
        js.setId(1L);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        BatchJob job = createGriffinJob();
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        given(jobRepo.countByJobNameAndDeleted(js.getJobName(), false)).willReturn(0);
        given(jobScheduleRepo.save(js)).willReturn(js);
        given(jobRepo.save(Matchers.any(BatchJob.class))).willReturn(job);

//        JobSchedule createdJs = service.addJob(js);
//        assertEquals(js.getJobName(), createdJs.getJobName());
    }

    @Test(expected = GriffinException.BadRequestException.class)
    public void testAddJobForFailureWithMeasureNull() throws Exception {
        JobSchedule js = createJobSchedule();
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(null);

        service.addJob(js);
    }

    @Test(expected = GriffinException.BadRequestException.class)
    public void testAddJobForFailureWitJobNameDuplicate() throws Exception {
        JobSchedule js = createJobSchedule();
        GriffinMeasure measure = createGriffinMeasure("measureName");
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        given(jobRepo.countByJobNameAndDeleted(js.getJobName(), false)).willReturn(1);

        service.addJob(js);
    }

    @Test(expected = GriffinException.BadRequestException.class)
    public void testAddJobForFailureWitJobNameNull() throws Exception {
        JobSchedule js = createJobSchedule(null);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);

        service.addJob(js);
    }

    @Test(expected = GriffinException.BadRequestException.class)
    public void testAddJobForFailureWithBaselineInvalid() throws Exception {
        JobDataSegment source = createJobDataSegment("source_name", false);
        JobDataSegment target = createJobDataSegment("target_name", false);
        JobSchedule js = createJobSchedule("jobName", source, target);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);

        service.addJob(js);
    }

    @Test(expected = GriffinException.BadRequestException.class)
    public void testAddJobForFailureWithConnectorNameInvalid() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDataSegment source = createJobDataSegment("source_connector_name", true);
        JobDataSegment target = createJobDataSegment("target_name", false);
        JobSchedule js = createJobSchedule("jobName", source, target);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);

        service.addJob(js);
    }

    @Test(expected = GriffinException.BadRequestException.class)
    public void testAddJobForFailureWithMeasureConnectorNameDuplicate() throws Exception {
        JobSchedule js = createJobSchedule();
        DataConnector dcSource = createDataConnector("connector_name", "default", "test_data_src", "dt=#YYYYMMdd# AND hour=#HH#");
        DataConnector dcTarget = createDataConnector("connector_name", "default", "test_data_tgt", "dt=#YYYYMMdd# AND hour=#HH#");
        GriffinMeasure measure = createGriffinMeasure("measureName", dcSource, dcTarget);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);

        service.addJob(js);
    }

    @Test(expected = GriffinException.BadRequestException.class)
    public void testAddJobForFailureWithJobScheduleConnectorNameRepeat() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDataSegment source = createJobDataSegment("source_name", true);
        JobDataSegment target = createJobDataSegment("source_name", false);
        JobSchedule js = createJobSchedule("jobName", source, target);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);

        service.addJob(js);
    }

    @Test(expected = GriffinException.ConflictException.class)
    public void testAddJobForFailureWithTriggerKeyExist() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDataSegment source = createJobDataSegment("source_name", true);
        JobDataSegment target = createJobDataSegment("target_name", false);
        JobSchedule js = createJobSchedule("jobName", source, target);
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(griffinMeasureRepo.findByIdAndDeleted(js.getMeasureId(), false)).willReturn(measure);
        given(scheduler.checkExists(Matchers.any(TriggerKey.class))).willReturn(true);

        service.addJob(js);
    }

    @Test
    public void testDeleteJobByIdForSuccessWithTriggerKeyExist() throws SchedulerException {
        Long jobId = 1L;
        BatchJob job = new BatchJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
//        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobInstanceRepo.findByJobId(Matchers.anyLong())).willReturn(Arrays.asList(instance));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);

        service.deleteJob(jobId);
        verify(scheduler, times(2)).checkExists(Matchers.any(JobKey.class));
        verify(scheduler, times(1)).pauseJob(Matchers.any(JobKey.class));
        verify(scheduler, times(1)).deleteJob(Matchers.any(JobKey.class));
        verify(jobRepo, times(1)).save(Matchers.any(BatchJob.class));
    }

    @Test
    public void testDeleteJobByIdForSuccessWithTriggerKeyNotExist() throws SchedulerException {
        Long jobId = 1L;
        BatchJob job = new BatchJob(1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
//        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(jobInstanceRepo.findByJobId(Matchers.anyLong())).willReturn(Arrays.asList(instance));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);

        service.deleteJob(jobId);
        verify(scheduler, times(2)).checkExists(Matchers.any(JobKey.class));
        verify(scheduler, times(0)).pauseJob(Matchers.any(JobKey.class));
        verify(scheduler, times(0)).deleteJob(Matchers.any(JobKey.class));
        verify(jobRepo, times(1)).save(Matchers.any(BatchJob.class));
    }

    @Test(expected = GriffinException.NotFoundException.class)
    public void testDeleteJobByIdForFailureWithJobNotFound() {
        given(jobRepo.findByIdAndDeleted(1L, false)).willReturn(null);

        service.deleteJob(1L);
    }

    @Test(expected = GriffinException.ServiceException.class)
    public void testDeleteJobByIdForFailureWithException() throws SchedulerException {
        Long jobId = 1L;
        BatchJob job = createGriffinJob();
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(Matchers.any(JobKey.class));

        service.deleteJob(jobId);
    }

    @Test
    public void testDeleteJobByNameForSuccessWithTriggerKeyExist() throws SchedulerException {
        BatchJob job = new BatchJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
//        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(jobRepo.findByJobNameAndDeleted(job.getJobName(), false)).willReturn(Arrays.asList(job));
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobInstanceRepo.findByJobId(Matchers.anyLong())).willReturn(Arrays.asList(instance));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doNothing().when(scheduler).pauseJob(Matchers.any(JobKey.class));
        given(scheduler.deleteJob(Matchers.any(JobKey.class))).willReturn(true);

        service.deleteJob(job.getJobName());
        verify(scheduler, times(2)).checkExists(Matchers.any(JobKey.class));
        verify(scheduler, times(1)).pauseJob(Matchers.any(JobKey.class));
        verify(scheduler, times(1)).deleteJob(Matchers.any(JobKey.class));
        verify(jobRepo, times(1)).save(Matchers.any(BatchJob.class));

    }

    @Test
    public void testDeleteJobByNameForSuccessWithTriggerKeyNotExist() throws SchedulerException {
        BatchJob job = new BatchJob(1L, 1L, "jobName", "quartzJobName", "quartzGroupName", false);
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
//        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByJobNameAndDeleted(job.getJobName(), false)).willReturn(Arrays.asList(job));
        given(jobInstanceRepo.findByJobId(Matchers.anyLong())).willReturn(Arrays.asList(instance));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);

        service.deleteJob(job.getJobName());
        verify(scheduler, times(2)).checkExists(Matchers.any(JobKey.class));
        verify(scheduler, times(0)).pauseJob(Matchers.any(JobKey.class));
        verify(scheduler, times(0)).deleteJob(Matchers.any(JobKey.class));
        verify(jobRepo, times(1)).save(Matchers.any(BatchJob.class));
    }

    @Test(expected = GriffinException.NotFoundException.class)
    public void testDeleteJobByJobNameForFailureWithJobNotFound() {
        String jobName = "jobName";
        given(jobRepo.findByJobNameAndDeleted(jobName, false)).willReturn(new ArrayList<>());

        service.deleteJob(jobName);
    }

    @Test(expected = GriffinException.ServiceException.class)
    public void testDeleteJobByJobNameForFailureWithException() throws SchedulerException {
        BatchJob job = createGriffinJob();
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByJobNameAndDeleted(job.getJobName(), false)).willReturn(Arrays.asList(job));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(Matchers.any(JobKey.class));

        service.deleteJob(job.getJobName());
    }

    @Test
    public void testDeleteJobsRelateToMeasureForSuccessWithTriggerKeyExist() throws SchedulerException {
        BatchJob job = createGriffinJob();
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
//        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(jobRepo.findByMeasureIdAndDeleted(1L, false)).willReturn(Arrays.asList(job));
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobInstanceRepo.findByJobId(Matchers.anyLong())).willReturn(Arrays.asList(instance));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);

        service.deleteJobsRelateToMeasure(1L);
        verify(scheduler, times(2)).checkExists(Matchers.any(JobKey.class));
        verify(scheduler, times(1)).pauseJob(Matchers.any(JobKey.class));
        verify(scheduler, times(1)).deleteJob(Matchers.any(JobKey.class));
        verify(jobRepo, times(1)).save(Matchers.any(BatchJob.class));
    }

    @Test
    public void testDeleteJobsRelateToMeasureForSuccessWithTriggerKeyNotExist() throws SchedulerException {
        BatchJob job = createGriffinJob();
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
//        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(jobRepo.findByMeasureIdAndDeleted(1L, false)).willReturn(Arrays.asList(job));
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobInstanceRepo.findByJobId(Matchers.anyLong())).willReturn(Arrays.asList(instance));
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);

        service.deleteJobsRelateToMeasure(1L);
        verify(scheduler, times(2)).checkExists(Matchers.any(JobKey.class));
        verify(scheduler, times(0)).pauseJob(Matchers.any(JobKey.class));
        verify(scheduler, times(0)).deleteJob(Matchers.any(JobKey.class));
        verify(jobRepo, times(1)).save(Matchers.any(BatchJob.class));
    }

    @Test
    public void testDeleteJobsRelateToMeasureForSuccessWithJobNotExist() {
        Long measureId = 1L;
        given(jobRepo.findByMeasureIdAndDeleted(measureId, false)).willReturn(null);

        service.deleteJobsRelateToMeasure(measureId);
        verify(jobRepo, times(1)).findByMeasureIdAndDeleted(measureId, false);
        verify(factory, times(0)).getScheduler();
    }

    @Test(expected = GriffinException.ServiceException.class)
    public void testDeleteJobsRelateToMeasureForFailureWithException() throws SchedulerException {
        Long measureId = 1L;
        BatchJob job = createGriffinJob();
        JobInstanceBean instance = new JobInstanceBean(LivySessionStates.State.finding, "pName", "pGroup", null, null);
//        job.setJobInstances(Arrays.asList(instance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(jobRepo.findByMeasureIdAndDeleted(measureId, false)).willReturn(Arrays.asList(job));
        given(factory.getScheduler()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(Matchers.any(JobKey.class));

        service.deleteJobsRelateToMeasure(measureId);
    }

    @Test
    public void testFindInstancesOfJobForSuccess() {
        Long jobId = 1L;
        int page = 0;
        int size = 2;
        BatchJob job = createGriffinJob();
        JobInstanceBean jobInstance = new JobInstanceBean(1L, LivySessionStates.State.dead, "app_id", "app_uri", null, null);
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(job);
        given(jobInstanceRepo.findByJobId(Matchers.anyLong(), Matchers.any(PageRequest.class))).willReturn(Arrays.asList(jobInstance));

        List<JobInstanceBean> jobInstanceBeans = service.findInstancesOfJob(1L, page, size);
        assertEquals(jobInstanceBeans.size(), 1);
    }

    @Test(expected = GriffinException.NotFoundException.class)
    public void testFindInstancesOfJobWithJobNotFound() {
        Long jobId = 1L;
        given(jobRepo.findByIdAndDeleted(jobId, false)).willReturn(null);

        service.findInstancesOfJob(jobId, 0, 2);
    }

    @Test
    public void testDeleteExpiredJobInstanceForSuccessWithTriggerKeyExist() throws SchedulerException {
        JobInstanceBean jobInstance = new JobInstanceBean(LivySessionStates.State.dead, "pName", "pGroup", null, null);
        given(jobInstanceRepo.findByExpireTmsLessThanEqual(Matchers.any())).willReturn(Arrays.asList(jobInstance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);

        service.deleteExpiredJobInstance();
        verify(scheduler, times(1)).pauseJob(Matchers.any(JobKey.class));
        verify(jobInstanceRepo, times(1)).deleteByExpireTimestamp(Matchers.any());
    }

    @Test
    public void testDeleteExpiredJobInstanceForSuccessWithTriggerKeyNotExist() throws SchedulerException {
        JobInstanceBean jobInstance = new JobInstanceBean(LivySessionStates.State.dead, "pName", "pGroup", null, null);
        given(jobInstanceRepo.findByExpireTmsLessThanEqual(Matchers.any())).willReturn(Arrays.asList(jobInstance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);

        service.deleteExpiredJobInstance();
        verify(scheduler, times(0)).pauseJob(Matchers.any(JobKey.class));
        verify(jobInstanceRepo, times(1)).deleteByExpireTimestamp(Matchers.any());
    }

    @Test
    public void testDeleteExpiredJobInstanceForSuccessWithNoInstance() {
        given(jobInstanceRepo.findByExpireTmsLessThanEqual(Matchers.any())).willReturn(null);

        service.deleteExpiredJobInstance();
        verify(jobInstanceRepo, times(1)).deleteByExpireTimestamp(Matchers.any());

    }

    @Test
    public void testDeleteExpiredJobInstanceForFailureWithException() throws SchedulerException {
        JobInstanceBean jobInstance = new JobInstanceBean(LivySessionStates.State.dead, "pName", "pGroup", null, null);
        given(jobInstanceRepo.findByExpireTmsLessThanEqual(Matchers.any())).willReturn(Arrays.asList(jobInstance));
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        given(factory.getScheduler()).willReturn(scheduler);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(true);
        doThrow(SchedulerException.class).when(scheduler).pauseJob(Matchers.any(JobKey.class));

        service.deleteExpiredJobInstance();
        verify(jobInstanceRepo, times(0)).deleteByExpireTimestamp(Matchers.any());
    }

    @Test
    public void testSyncInstancesOfJobForSuccess() {
        JobInstanceBean instance = createJobInstance();
        LivySessionStates.State[] states = {starting, not_started, recovering, idle, running, busy};
        given(jobInstanceRepo.findByActiveState(states)).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        String result = "{\"id\":1,\"state\":\"starting\",\"appId\":123,\"appInfo\":{\"driverLogUrl\":null,\"sparkUiUrl\":null},\"log\":[]}";
//        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn(result);

        service.syncInstancesOfAllJobs();
        verify(jobInstanceRepo, times(1)).save(instance);
    }

    @Test
    public void testSyncInstancesOfJobForFailureWithRestClientException() {
        JobInstanceBean instance = createJobInstance();
        instance.setSessionId(1234564L);
        LivySessionStates.State[] states = {starting, not_started, recovering, idle, running, busy};
        given(jobInstanceRepo.findByActiveState(states)).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willThrow(RestClientException.class);

        service.syncInstancesOfAllJobs();
        verify(jobInstanceRepo, times(1)).save(instance);
    }

    @Test
    public void testSyncInstancesOfJobForFailureWithIOException() {
        JobInstanceBean instance = createJobInstance();
        LivySessionStates.State[] states = {starting, not_started, recovering, idle, running, busy};
        given(jobInstanceRepo.findByActiveState(states)).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn("result");

        service.syncInstancesOfAllJobs();
        verify(jobInstanceRepo, times(0)).save(instance);
    }

    @Test
    public void testSyncInstancesOfJobForFailureWithIllegalArgumentException() {
        JobInstanceBean instance = createJobInstance();
        LivySessionStates.State[] states = {starting, not_started, recovering, idle, running, busy};
        given(jobInstanceRepo.findByActiveState(states)).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn("{\"state\":\"wrong\"}");

        service.syncInstancesOfAllJobs();
        verify(jobInstanceRepo, times(0)).save(instance);
    }

    @Test
    public void testSyncInstancesOfJobForFailureWithException() {
        JobInstanceBean instance = createJobInstance();
        LivySessionStates.State[] states = {starting, not_started, recovering, idle, running, busy};
        given(jobInstanceRepo.findByActiveState(states)).willReturn(Arrays.asList(instance));
        Whitebox.setInternalState(service, "restTemplate", restTemplate);
        String result = "{\"id\":1,\"state\":\"starting\",\"appId\":123,\"appInfo\":{\"driverLogUrl\":null,\"sparkUiUrl\":null},\"log\":[]}";
        given(restTemplate.getForObject(Matchers.anyString(), Matchers.any())).willReturn(result);
        doThrow(Exception.class).when(jobInstanceRepo).save(Matchers.any(JobInstanceBean.class));

        service.syncInstancesOfAllJobs();
        verify(restTemplate, times(1)).getForObject(Matchers.anyString(), Matchers.any());
        verify(sparkJobProps, times(2)).getProperty(Matchers.anyString());
    }

    @Test
    public void testGetHealthInfoWithHealthy() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        BatchJob job = createGriffinJob();
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Arrays.asList(job));
        List<Trigger> triggers = Collections.singletonList(new SimpleTriggerImpl());
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(triggers);
        given(jobInstanceRepo.findByJobId(Matchers.anyLong(), Matchers.any(PageRequest.class)))
                .willReturn(Collections.singletonList(createJobInstance()));

        assertEquals(service.getHealthInfo().getHealthyJobCount(), 1);

    }

    @Test
    public void testGetHealthInfoWithUnhealthy() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        BatchJob job = createGriffinJob();
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Collections.singletonList(job));
        List<Trigger> triggers = Collections.singletonList(new SimpleTriggerImpl());
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(triggers);
        JobInstanceBean instance = createJobInstance();
        instance.setState(LivySessionStates.State.error);
        List<JobInstanceBean> scheduleStateList = Collections.singletonList(instance);
        given(jobInstanceRepo.findByJobId(Matchers.anyLong(), Matchers.any(PageRequest.class)))
                .willReturn(scheduleStateList);

        assertEquals(service.getHealthInfo().getHealthyJobCount(), 0);
    }

    @Test(expected = GriffinException.ServiceException.class)
    public void testGetHealthInfoWithException() throws SchedulerException {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        BatchJob job = createGriffinJob();
        given(factory.getScheduler()).willReturn(scheduler);
        given(jobRepo.findByDeleted(false)).willReturn(Collections.singletonList(job));
        given((List<Trigger>) scheduler.getTriggersOfJob(Matchers.any(JobKey.class)))
                .willThrow(new SchedulerException());

        service.getHealthInfo();
    }

}
