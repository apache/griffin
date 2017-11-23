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
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.job.entity.SegmentPredict;
import org.apache.griffin.core.job.entity.SparkJobDO;
import org.apache.griffin.core.job.factory.PredictorFactory;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.util.JsonUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;

import static org.apache.griffin.core.job.PredictJob.*;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class SparkSubmitJob implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSubmitJob.class);

    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    private Properties sparkJobProps;
    @Autowired
    private JobServiceImpl jobService;

    private Measure measure;
    private String livyUri;
    private List<SegmentPredict> mPredicts;
    private RestTemplate restTemplate = new RestTemplate();
    private SparkJobDO sparkJobDO = new SparkJobDO();

    public SparkSubmitJob() {
    }

    @Override
    public void execute(JobExecutionContext context) {
        JobDetail jobDetail = context.getJobDetail();
        String result;
        try {
            initParam(jobDetail);
            setSparkJobDO();
            if (predict(mPredicts)) {
                result = restTemplate.postForObject(livyUri, sparkJobDO, String.class);
                LOGGER.info(result);
                JobDataMap jobDataMap = jobDetail.getJobDataMap();
                saveJobInstance(jobDataMap.getString(GROUP_NAME_KEY), jobDataMap.getString(JOB_NAME_KEY), result);
                jobService.deleteJob(jobDetail.getKey().getGroup(), jobDetail.getKey().getName());

            }
        } catch (Exception e) {
            LOGGER.error("Post spark task error.", e);
        }
    }

    private boolean predict(List<SegmentPredict> predicts) throws IOException {
        if (predicts == null) {
            return true;
        }
        for (SegmentPredict segmentPredict : predicts) {
            Predictor predict = PredictorFactory.newPredictInstance(segmentPredict);
            if (!predict.predict()) {
                return false;
            }
        }
        return true;
    }


    private void initParam(JobDetail jd) throws IOException {
        mPredicts = new ArrayList<>();
        livyUri = sparkJobProps.getProperty("livy.uri");
        measure = JsonUtil.toEntity(jd.getJobDataMap().getString(MEASURE_KEY), Measure.class);
        initPredicts(jd.getJobDataMap().getString(PREDICTS_KEY));
        setMeasureInstanceName(measure, jd);

    }

    private void initPredicts(String json) throws IOException {
        if (StringUtils.isEmpty(json)) {
            return;
        }
        List<Map> maps = JsonUtil.toEntity(json, List.class);
        for (Map<String, String> map : maps) {
            SegmentPredict segmentPredict = new SegmentPredict();
            segmentPredict.setType(map.get("type"));
            segmentPredict.setConfig(JsonUtil.toEntity(map.get("config"), Map.class));
            mPredicts.add(segmentPredict);
        }
    }

    private void setMeasureInstanceName(Measure measure, JobDetail jd) {
        // in order to keep metric name unique, we set measure name as jobName at present
        measure.setName(jd.getJobDataMap().getString("jobName"));
    }

    private String escapeCharacter(String str, String regex) {
        String escapeCh = "\\" + regex;
        return str.replaceAll(regex, escapeCh);
    }

    private void setSparkJobDO() throws JsonProcessingException {
        sparkJobDO.setFile(sparkJobProps.getProperty("sparkJob.file"));
        sparkJobDO.setClassName(sparkJobProps.getProperty("sparkJob.className"));

        List<String> args = new ArrayList<>();
        args.add(sparkJobProps.getProperty("sparkJob.args_1"));
        measure.setTriggerTimeStamp(System.currentTimeMillis());
        String measureJson = JsonUtil.toJsonWithFormat(measure);
        // to fix livy bug: ` will be ignored by livy
        String finalMeasureJson = escapeCharacter(measureJson, "\\`");
        args.add(finalMeasureJson);
        args.add(sparkJobProps.getProperty("sparkJob.args_3"));
        sparkJobDO.setArgs(args);

        sparkJobDO.setName(sparkJobProps.getProperty("sparkJob.name"));
        sparkJobDO.setQueue(sparkJobProps.getProperty("sparkJob.queue"));
        sparkJobDO.setNumExecutors(Long.parseLong(sparkJobProps.getProperty("sparkJob.numExecutors")));
        sparkJobDO.setExecutorCores(Long.parseLong(sparkJobProps.getProperty("sparkJob.executorCores")));
        sparkJobDO.setDriverMemory(sparkJobProps.getProperty("sparkJob.driverMemory"));
        sparkJobDO.setExecutorMemory(sparkJobProps.getProperty("sparkJob.executorMemory"));

        Map<String, String> conf = new HashMap<>();
        conf.put("spark.jars.packages", sparkJobProps.getProperty("sparkJob.spark.jars.packages"));
        sparkJobDO.setConf(conf);

        List<String> jars = new ArrayList<>();
        jars.add(sparkJobProps.getProperty("sparkJob.jars_1"));
        jars.add(sparkJobProps.getProperty("sparkJob.jars_2"));
        jars.add(sparkJobProps.getProperty("sparkJob.jars_3"));
        sparkJobDO.setJars(jars);

        List<String> files = new ArrayList<>();
        sparkJobDO.setFiles(files);
    }

    private void saveJobInstance(String groupName, String jobName, String result) {
        TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {
        };
        try {
            Map<String, Object> resultMap = JsonUtil.toEntity(result, type);
            if (resultMap != null) {
                JobInstance jobInstance = genJobInstance(groupName, jobName, resultMap);
                jobInstanceRepo.save(jobInstance);
            }
        } catch (IOException e) {
            LOGGER.error("jobInstance jsonStr convert to map failed. {}", e.getMessage());
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Livy status is illegal. {}", e.getMessage());
        }
    }

    private JobInstance genJobInstance(String groupName, String jobName, Map<String, Object> resultMap) throws IllegalArgumentException {
        JobInstance jobInstance = new JobInstance();
        jobInstance.setGroupName(groupName);
        jobInstance.setJobName(jobName);
        jobInstance.setTimestamp(System.currentTimeMillis());
        if (resultMap.get("state") != null) {
            jobInstance.setState(LivySessionStates.State.valueOf(resultMap.get("state").toString()));
        }
        if (resultMap.get("id") != null) {
            jobInstance.setSessionId(Integer.parseInt(resultMap.get("id").toString()));
        }
        if (resultMap.get("appId") != null) {
            jobInstance.setAppId(resultMap.get("appId").toString());
        }
        return jobInstance;
    }

}
