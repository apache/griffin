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

package org.apache.griffin.core.service;

import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;


@RunWith(SpringRunner.class)
@WebMvcTest(value = GriffinController.class)
public class GriffinControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    MeasureRepo measureRepo;


    @Before
    public void setup() {

    }

    @Test
    public void testGreeting() throws Exception {
        mockMvc.perform(get("/version"))
                .andExpect(status().isOk())
                .andExpect(content().string(is("0.1.0")));
    }

    @Test
    public void testGetOrgs() throws Exception {
        when(measureRepo.findOrganizations()).thenReturn(Arrays.asList("ebay"));
        mockMvc.perform(get("/org"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0]", is("ebay")));
    }

    @Test
    public void testGetMetricNameListByOrg() throws Exception {
        String org = "hadoop";
        when(measureRepo.findNameByOrganization(org)).thenReturn(Arrays.asList(org));
        mockMvc.perform(get("/org/{org}", org))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0]", is(org)));
    }


}
