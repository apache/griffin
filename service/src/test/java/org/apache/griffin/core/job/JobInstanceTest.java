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

import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.job.repo.GriffinJobRepo;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.job.repo.JobScheduleRepo;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.repo.GriffinMeasureRepo;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.griffin.core.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.junit4.SpringRunner;

import javax.validation.constraints.AssertTrue;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.griffin.core.util.EntityHelper.*;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@RunWith(SpringRunner.class)
public class JobInstanceTest {

    @TestConfiguration
    public static class jobInstanceBean{
        @Bean
        public JobInstance instance() {
            return new JobInstance();
        }

        @Bean(name = "appConf")
        public Properties sparkJobProps() {
            String path = "application.properties";
            return PropertiesUtil.getProperties(path, new ClassPathResource(path));
        }

        @Bean(name = "schedulerFactoryBean")
        public SchedulerFactoryBean factoryBean() {
            return new SchedulerFactoryBean();
        }
    }

    @Autowired
    private JobInstance jobInstance;

    @Autowired
    @Qualifier("appConf")
    private Properties appConfProps;

    @MockBean
    private JobInstanceRepo instanceRepo;

    @MockBean
    private SchedulerFactoryBean factory;

    @MockBean
    private GriffinMeasureRepo measureRepo;

    @MockBean
    private GriffinJobRepo jobRepo;

    @MockBean
    private JobScheduleRepo jobScheduleRepo;



    @Test
    public void testExecute() throws Exception {
        JobExecutionContext context = mock(JobExecutionContext.class);
        Scheduler scheduler = mock(Scheduler.class);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDetail jd = createJobDetail(JsonUtil.toJson(measure), "");
        JobSchedule jobSchedule = createJobSchedule();
        GriffinJob job = new GriffinJob(1L, "jobName", "qName", "qGroup", false);
        List<Trigger> triggers = Arrays.asList(createSimpleTrigger(2, 0));
        given(context.getJobDetail()).willReturn(jd);
        given(jobScheduleRepo.findOne(Matchers.anyLong())).willReturn(jobSchedule);
        given(measureRepo.findOne(Matchers.anyLong())).willReturn(measure);
        given(jobRepo.findOne(Matchers.anyLong())).willReturn(job);
        given(factory.getScheduler()).willReturn(scheduler);
        given((List<Trigger>)scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(triggers);
        given(scheduler.checkExists(Matchers.any(TriggerKey.class))).willReturn(false);
        given(jobRepo.save(Matchers.any(GriffinJob.class))).willReturn(job);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);
        jobInstance.execute(context);
    }

    @Test
    public void testExecuteWithRangeLessThanZero() throws Exception {
        JobExecutionContext context = mock(JobExecutionContext.class);
        Scheduler scheduler = mock(Scheduler.class);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDetail jd = createJobDetail(JsonUtil.toJson(measure), "");
        JobSchedule jobSchedule = createJobSchedule("jobName",new SegmentRange("-1h","-1h"));
        GriffinJob job = new GriffinJob(1L, "jobName", "qName", "qGroup", false);
        List<Trigger> triggers = Arrays.asList(createSimpleTrigger(2, 0));
        given(context.getJobDetail()).willReturn(jd);
        given(jobScheduleRepo.findOne(Matchers.anyLong())).willReturn(jobSchedule);
        given(measureRepo.findOne(Matchers.anyLong())).willReturn(measure);
        given(jobRepo.findOne(Matchers.anyLong())).willReturn(job);
        given(factory.getScheduler()).willReturn(scheduler);
        given((List<Trigger>)scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(triggers);
        given(scheduler.checkExists(Matchers.any(TriggerKey.class))).willReturn(false);
        given(jobRepo.save(Matchers.any(GriffinJob.class))).willReturn(job);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);
        jobInstance.execute(context);
    }

    @Test
    public void testExecuteWithRangeGreaterThanDataUnit() throws Exception {
        JobExecutionContext context = mock(JobExecutionContext.class);
        Scheduler scheduler = mock(Scheduler.class);
        GriffinMeasure measure = createGriffinMeasure("measureName");
        JobDetail jd = createJobDetail(JsonUtil.toJson(measure), "");
        JobSchedule jobSchedule = createJobSchedule("jobName",new SegmentRange("-1h","5h"));
        GriffinJob job = new GriffinJob(1L, "jobName", "qName", "qGroup", false);
        List<Trigger> triggers = Arrays.asList(createSimpleTrigger(2, 0));
        given(context.getJobDetail()).willReturn(jd);
        given(jobScheduleRepo.findOne(Matchers.anyLong())).willReturn(jobSchedule);
        given(measureRepo.findOne(Matchers.anyLong())).willReturn(measure);
        given(jobRepo.findOne(Matchers.anyLong())).willReturn(job);
        given(factory.getScheduler()).willReturn(scheduler);
        given((List<Trigger>)scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(triggers);
        given(scheduler.checkExists(Matchers.any(TriggerKey.class))).willReturn(false);
        given(jobRepo.save(Matchers.any(GriffinJob.class))).willReturn(job);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);
        jobInstance.execute(context);
    }

    @Test
    public void testExecuteWithPredicate() throws Exception {
        JobExecutionContext context = mock(JobExecutionContext.class);
        Scheduler scheduler = mock(Scheduler.class);
        GriffinMeasure measure = createGriffinMeasure("measureName",createFileExistPredicate(),createFileExistPredicate());
        JobDetail jd = createJobDetail(JsonUtil.toJson(measure), "");
        JobSchedule jobSchedule = createJobSchedule("jobName");
        GriffinJob job = new GriffinJob(1L, "jobName", "qName", "qGroup", false);
        List<Trigger> triggers = Arrays.asList(createSimpleTrigger(2, 0));
        given(context.getJobDetail()).willReturn(jd);
        given(jobScheduleRepo.findOne(Matchers.anyLong())).willReturn(jobSchedule);
        given(measureRepo.findOne(Matchers.anyLong())).willReturn(measure);
        given(jobRepo.findOne(Matchers.anyLong())).willReturn(job);
        given(factory.getScheduler()).willReturn(scheduler);
        given((List<Trigger>)scheduler.getTriggersOfJob(Matchers.any(JobKey.class))).willReturn(triggers);
        given(scheduler.checkExists(Matchers.any(TriggerKey.class))).willReturn(false);
        given(jobRepo.save(Matchers.any(GriffinJob.class))).willReturn(job);
        given(scheduler.checkExists(Matchers.any(JobKey.class))).willReturn(false);
        jobInstance.execute(context);
    }

    @Test
    public void testExecuteWithNullException() throws Exception {
        JobExecutionContext context = mock(JobExecutionContext.class);
        jobInstance.execute(context);
        assertTrue(true);
    }

}