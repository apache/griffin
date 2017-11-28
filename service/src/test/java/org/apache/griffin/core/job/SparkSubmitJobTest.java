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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


//@RunWith(SpringRunner.class)
//public class SparkSubmitJobTest {

//    @TestConfiguration
//    public static class SchedulerServiceConfiguration {
//        @Bean
//        public SparkSubmitJob sparkSubmitJobBean() {
//            return new SparkSubmitJob();
//        }
//
//        @Bean
//        public Properties sparkJobProps() {
//            return PropertiesUtil.getProperties("/sparkJob.properties");
//        }
//
//    }
//
//    @Autowired
//    private SparkSubmitJob sparkSubmitJob;
//
//    @MockBean
//    private MeasureRepo measureRepo;
//
//    @MockBean
//    private RestTemplate restTemplate;
//
//    @MockBean
//    private JobInstanceRepo jobInstanceRepo;
//
//    @Before
//    public void setUp() {
//    }
//
//    @Test
//    public void testExecute() throws Exception {
//        String result = "{\"id\":1,\"state\":\"starting\",\"appId\":null,\"appInfo\":{\"driverLogUrl\":null,\"sparkUiUrl\":null},\"log\":[]}";
//        JobExecutionContext context = mock(JobExecutionContext.class);
//        JobDetail jd = createJobDetail();
//        given(context.getJobDetail()).willReturn(jd);
//        given(measureRepo.findOne(Long.valueOf(jd.getJobDataMap().getString("measureId")))).willReturn(createATestMeasure("view_item_hourly", "ebay"));
//        Whitebox.setInternalState(sparkSubmitJob, "restTemplate", restTemplate);
//        given(restTemplate.postForObject(Matchers.anyString(), Matchers.any(), Matchers.any())).willReturn(result);
//        given(jobInstanceRepo.save(new JobInstanceBean())).willReturn(new JobInstanceBean());
//        sparkSubmitJob.execute(context);
//        assertTrue(true);
//    }

//}
