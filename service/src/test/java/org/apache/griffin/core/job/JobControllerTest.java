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
import org.apache.griffin.core.exception.GriffinException;
import org.apache.griffin.core.exception.GriffinExceptionHandler;
import org.apache.griffin.core.exception.GriffinExceptionMessage;
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.griffin.core.util.URLHelper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.griffin.core.exception.GriffinExceptionMessage.JOB_ID_DOES_NOT_EXIST;
import static org.apache.griffin.core.exception.GriffinExceptionMessage.JOB_NAME_DOES_NOT_EXIST;
import static org.apache.griffin.core.util.EntityHelper.createGriffinJob;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
public class JobControllerTest {

    private MockMvc mvc;

    @Mock
    private JobServiceImpl service;

    @InjectMocks
    private JobController controller;

    @Before
    public void setup() {
        mvc = MockMvcBuilders
                .standaloneSetup(controller)
                .setControllerAdvice(new GriffinExceptionHandler())
                .build();
    }


    @Test
    public void testGetJobs() throws Exception {
        JobDataBean jobBean = new JobDataBean();
        jobBean.setJobName("job_name");
        given(service.getAliveJobs()).willReturn(Collections.singletonList(jobBean));

        mvc.perform(get(URLHelper.API_VERSION_PATH + "/jobs").contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].jobName", is("job_name")));
    }

    @Test
    public void testAddJobForSuccess() throws Exception {
        JobSchedule jobSchedule = getJobSchedule();
        GriffinJob job = createGriffinJob();
        given(service.addJob(jobSchedule)).willReturn(job);

        mvc.perform(post(URLHelper.API_VERSION_PATH + "/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.toJson(jobSchedule)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id", is(1)));
    }

    @Test
    public void testAddJobForFailureWithBadRequest() throws Exception {
        JobSchedule jobSchedule = getJobSchedule();
        given(service.addJob(jobSchedule))
                .willThrow(new GriffinException.BadRequestException(GriffinExceptionMessage.MISSING_METRIC_NAME));

        mvc.perform(post(URLHelper.API_VERSION_PATH + "/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.toJson(jobSchedule)))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void testAddJobForFailureWithTriggerKeyExist() throws Exception {
        JobSchedule jobSchedule = getJobSchedule();
        given(service.addJob(jobSchedule))
                .willThrow(new GriffinException.ConflictException(GriffinExceptionMessage.QUARTZ_JOB_ALREADY_EXIST));

        mvc.perform(post(URLHelper.API_VERSION_PATH + "/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.toJson(jobSchedule)))
                .andExpect(status().isConflict());
    }

    private JobSchedule getJobSchedule() throws JsonProcessingException {
        return new JobSchedule(1L, "jobName", "0 0/4 * * * ?", "GMT+8:00", null);
    }

    @Test
    public void testDeleteJobByIdForSuccess() throws Exception {
        doNothing().when(service).deleteJob(1L);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/jobs/1"))
                .andExpect(status().isNoContent());
    }

    @Test
    public void testDeleteJobByIdForFailureWithNotFound() throws Exception {
        doThrow(new GriffinException.NotFoundException(JOB_ID_DOES_NOT_EXIST)).when(service).deleteJob(1L);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/jobs/1"))
                .andExpect(status().isNotFound());
    }

    @Test
    public void testDeleteJobByIdForFailureWithException() throws Exception {
        doThrow(new GriffinException.ServiceException("Failed to delete job", new Exception()))
                .when(service).deleteJob(1L);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/jobs/1"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testDeleteJobByNameForSuccess() throws Exception {
        String jobName = "jobName";
        doNothing().when(service).deleteJob(jobName);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/jobs").param("jobName", jobName))
                .andExpect(status().isNoContent());
    }

    @Test
    public void testDeleteJobByNameForFailureWithNotFound() throws Exception {
        String jobName = "jobName";
        doThrow(new GriffinException.NotFoundException(JOB_NAME_DOES_NOT_EXIST)).when(service).deleteJob(jobName);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/jobs").param("jobName", jobName))
                .andExpect(status().isNotFound());
    }

    @Test
    public void testDeleteJobByNameForFailureWithException() throws Exception {
        String jobName = "jobName";
        doThrow(new GriffinException.ServiceException("Failed to delete job", new Exception()))
                .when(service).deleteJob(jobName);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/jobs").param("jobName", jobName))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testFindInstancesOfJob() throws Exception {
        int page = 0;
        int size = 2;
        JobInstanceBean jobInstance = new JobInstanceBean(1L, LivySessionStates.State.running, "", "", null, null);
        given(service.findInstancesOfJob(1L, page, size)).willReturn(Arrays.asList(jobInstance));

        mvc.perform(get(URLHelper.API_VERSION_PATH + "/jobs/instances").param("jobId", String.valueOf(1L))
                .param("page", String.valueOf(page)).param("size", String.valueOf(size)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].state", is("running")));
    }

    @Test
    public void testGetHealthInfo() throws Exception {
        JobHealth jobHealth = new JobHealth(1, 3);
        given(service.getHealthInfo()).willReturn(jobHealth);

        mvc.perform(get(URLHelper.API_VERSION_PATH + "/jobs/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.healthyJobCount", is(1)));
    }
}
