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
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.apache.griffin.core.util.URLHelper;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest(value = JobController.class, secure = false)
public class JobControllerTest {
    @Autowired
    private MockMvc mvc;

    @MockBean
    private JobService service;

    @Before
    public void setup() {
    }


    @Test
    public void testGetJobs() throws Exception {
        Map<String, Serializable> map = new HashMap<>();
        map.put("jobName", "job1");
        map.put("groupName", "BA");
        given(service.getAliveJobs()).willReturn(Arrays.asList(map));

        mvc.perform(get(URLHelper.API_VERSION_PATH + "/jobs/").contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].jobName", is("job1")));
    }

    @Test
    public void testAddJobForSuccess() throws Exception {
        String groupName = "BA";
        String jobName = "job1";
        long measureId = 0;
        JobRequestBody jobRequestBody = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH", "111", "20170607", "100");
        String schedulerRequestBodyJson = new ObjectMapper().writeValueAsString(jobRequestBody);
        given(service.addJob(groupName, jobName, measureId, jobRequestBody)).willReturn(GriffinOperationMessage.CREATE_JOB_SUCCESS);

        mvc.perform(post(URLHelper.API_VERSION_PATH + "/jobs").param("group", groupName).param("jobName", jobName)
                .param("measureId", String.valueOf(measureId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(schedulerRequestBodyJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(205)))
                .andExpect(jsonPath("$.description", is("Create Job Succeed")))
                .andDo(print());
    }

    @Test
    public void testAddJobForFail() throws Exception {
        String groupName = "BA";
        String jobName = "job1";
        long measureId = 0;
        JobRequestBody jobRequestBody = new JobRequestBody("YYYYMMdd-HH", "YYYYMMdd-HH", "111", "20170607", "100");
        String schedulerRequestBodyJson = new ObjectMapper().writeValueAsString(jobRequestBody);
        given(service.addJob(groupName, jobName, measureId, jobRequestBody)).willReturn(GriffinOperationMessage.CREATE_JOB_FAIL);

        mvc.perform(post(URLHelper.API_VERSION_PATH + "/jobs").param("group", groupName).param("jobName", jobName)
                .param("measureId", String.valueOf(measureId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(schedulerRequestBodyJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(405)))
                .andExpect(jsonPath("$.description", is("Create Job Failed")))
                .andDo(print());
    }

    @Test
    public void testDeleteJobForSuccess() throws Exception {
        String groupName = "BA";
        String jobName = "job1";
        given(service.deleteJob(groupName, jobName)).willReturn(GriffinOperationMessage.DELETE_JOB_SUCCESS);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/jobs").param("group", groupName).param("jobName", jobName))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(206)))
                .andExpect(jsonPath("$.description", is("Delete Job Succeed")));
    }

    @Test
    public void testDeleteJobForFail() throws Exception {
        String groupName = "BA";
        String jobName = "job1";
        given(service.deleteJob(groupName, jobName)).willReturn(GriffinOperationMessage.DELETE_JOB_FAIL);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/jobs").param("group", groupName).param("jobName", jobName))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(406)))
                .andExpect(jsonPath("$.description", is("Delete Job Failed")));
    }

    @Test
    public void testFindInstancesOfJob() throws Exception {
        String groupName = "BA";
        String jobName = "job1";
        int page = 0;
        int size = 2;
        JobInstance jobInstance = new JobInstance(groupName, jobName, 1, LivySessionStates.State.running, "", "", System.currentTimeMillis());
        given(service.findInstancesOfJob(groupName, jobName, page, size)).willReturn(Arrays.asList(jobInstance));

        mvc.perform(get(URLHelper.API_VERSION_PATH + "/jobs/instances").param("group", groupName).param("jobName", jobName)
                .param("page", String.valueOf(page)).param("size", String.valueOf(size)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].groupName", is("BA")));
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
