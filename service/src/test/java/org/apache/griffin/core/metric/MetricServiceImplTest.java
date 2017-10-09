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

package org.apache.griffin.core.metric;

import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

@RunWith(SpringRunner.class)
public class MetricServiceImplTest {
    @TestConfiguration
    static class MetricServiceConfiguration {
        @Bean
        public MetricServiceImpl service() {
            return new MetricServiceImpl();
        }
    }

    @MockBean
    private MeasureRepo measureRepo;

    @Autowired
    private MetricServiceImpl service;

    @Before
    public void setup() {
    }

    @Test
    public void testGetOrgByMeasureName() {
        String measureName = "default";
        String org = "ebay";
        given(measureRepo.findOrgByName("default")).willReturn(org);
        assertEquals(service.getOrgByMeasureName(measureName), org);
    }
}
