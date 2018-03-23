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
import org.apache.griffin.core.job.repo.GriffinStreamingJobRepo;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.util.YarnNetUtil;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;

import static org.apache.griffin.core.exception.GriffinExceptionMessage.INVALID_JOB_NAME;
import static org.apache.griffin.core.exception.GriffinExceptionMessage.STREAMING_JOB_IS_RUNNING;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.stopped;
import static org.apache.griffin.core.job.entity.LivySessionStates.isActive;
import static org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType.streaming;
import static org.quartz.TriggerKey.triggerKey;

@Service
public class StreamingJobOperationImpl implements JobOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingJobOperationImpl.class);
    @Autowired
    private GriffinStreamingJobRepo streamingJobRepo;
    @Autowired
    @Qualifier("livyConf")
    private Properties livyConfProps;
    @Autowired
    private JobServiceImpl jobService;

    private String livyUri;
    private RestTemplate restTemplate;

    @PostConstruct
    public void init() {
        restTemplate = new RestTemplate();
        livyUri = livyConfProps.getProperty("livy.uri");
    }

    //TODO Also you should validate transactional whether working

    @Override
    @Transactional(rollbackFor = Exception.class)
    public AbstractJob add(JobSchedule js, GriffinMeasure measure) throws Exception {
        validateParams(js);
        String qName = jobService.getQuartzName(js);
        String qGroup = jobService.getQuartzGroup();
        TriggerKey triggerKey = jobService.getTriggerKeyIfValid(qName, qGroup);
        StreamingJob streamingJob = new StreamingJob(js.getJobName(), js.getMeasureId(), js.getJobName());
        streamingJob.setJobSchedule(js);
        streamingJobRepo.save(streamingJob);
        jobService.addJob(triggerKey, js, streamingJob, streaming);
        return streamingJob;
    }

    /**
     * active state: not_started, starting, recovering, idle, running, busy
     * inactive state: shutting_down, error, dead, success
     *
     * @param job streaming job
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void start(AbstractJob job) {
        StreamingJob streamingJob = (StreamingJob) job;
        setJobInstanceState(streamingJob);
        streamingJobRepo.save(streamingJob);
        JobSchedule js = streamingJob.getJobSchedule();
        String qName = jobService.getQuartzName(js);
        String qGroup = jobService.getQuartzGroup();
        TriggerKey triggerKey = triggerKey(qName, qGroup);
        try {
            jobService.addJob(triggerKey, js, streamingJob, streaming);
        } catch (Exception e) {
            LOGGER.error("Failed to start job", e);
            throw new GriffinException.ServiceException("Failed to start job", e);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void stop(AbstractJob job) {
        StreamingJob streamingJob = (StreamingJob) job;
        stop(streamingJob, false);
    }

    @Override
    public void delete(AbstractJob job) {
        StreamingJob streamingJob = (StreamingJob) job;
        stop(streamingJob, true);
    }

    @Override
    public JobDataBean getJobData(AbstractJob job) {
        StreamingJob streamingJob = (StreamingJob) job;
        JobDataBean data = new JobDataBean();
        data.setJobId(streamingJob.getId());
        data.setJobName(streamingJob.getJobName());
        data.setMeasureId(streamingJob.getMeasureId());
        data.setType(streaming);
        List<JobInstanceBean> instances = streamingJob.getJobInstances();
        for (JobInstanceBean instance : instances) {
            if (!instance.getDeleted()) {
                data.setState(instance.getState().toString());
                break;
            }
        }
        return data;
    }

    @Override
    public JobHealth getHealthInfo(JobHealth jobHealth, AbstractJob job) {
        jobHealth.setJobCount(jobHealth.getJobCount() + 1);
        if (jobService.isJobHealthy(job.getId())) {
            jobHealth.setHealthyJobCount(jobHealth.getHealthyJobCount() + 1);
        }
        return jobHealth;
    }

    private void deleteByLivy(JobInstanceBean instance) {
        String url = livyUri + "/" + instance.getSessionId();
        try {
            restTemplate.delete(url);
        } catch (RestClientException ex) {
            LOGGER.error("url:{} happens exception by livy. {}", url, ex.getMessage());
            YarnNetUtil.delete(livyUri, instance.getAppId());
        }
    }

    private void setJobInstanceState(StreamingJob streamingJob) {
        List<JobInstanceBean> instances = streamingJob.getJobInstances();
        for (JobInstanceBean instance : instances) {
            if (!instance.getDeleted() && isActive(instance.getState())) {
                throw new GriffinException.BadRequestException(STREAMING_JOB_IS_RUNNING);
            } else if (!instance.getDeleted()) {
                instance.setDeleted(true);
            }
        }
    }

    private void stop(StreamingJob job, boolean isDelete) {
        List<JobInstanceBean> instances = job.getJobInstances();
        for (JobInstanceBean instance : instances) {
            if (!instance.getDeleted()) {
                if (isActive(instance.getState())) {
                    deleteByLivy(instance);
                    instance.setState(stopped);
                }
                instance.setDeleted(true);
            }
        }
        job.setDeleted(isDelete);
        streamingJobRepo.save(job);
    }

    private void validateParams(JobSchedule js) {
        if (!jobService.isValidJobName(js.getJobName())) {
            throw new GriffinException.BadRequestException(INVALID_JOB_NAME);
        }
    }

}
