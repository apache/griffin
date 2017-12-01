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
import org.apache.griffin.core.job.entity.JobInstanceBean;
import org.apache.griffin.core.job.entity.LivyConf;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.job.entity.SegmentPredicate;
import org.apache.griffin.core.job.factory.PredicatorFactory;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.util.JsonUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;

import static org.apache.griffin.core.job.JobInstance.*;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class SparkSubmitJob implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSubmitJob.class);
    public static final String SPARK_JOB_JARS_SPLIT = ";";

    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    private Properties livyConfProps;
    @Autowired
    private JobServiceImpl jobService;

    private Measure measure;
    private String livyUri;
    private List<SegmentPredicate> mPredicts;
    private RestTemplate restTemplate = new RestTemplate();
    private LivyConf livyConf = new LivyConf();

    @Override
    public void execute(JobExecutionContext context) {
        JobDetail jobDetail = context.getJobDetail();
        String result;
        try {
            initParam(jobDetail);
            setLivyConf();
            if (success(mPredicts)) {
                result = restTemplate.postForObject(livyUri, livyConf, String.class);
                LOGGER.info(result);
                JobDataMap jobDataMap = jobDetail.getJobDataMap();
                saveJobInstance(jobDataMap.getString(GROUP_NAME_KEY), jobDataMap.getString(JOB_NAME_KEY), result);
                jobService.deleteJob(jobDetail.getKey().getGroup(), jobDetail.getKey().getName());

            }
        } catch (Exception e) {
            LOGGER.error("Post spark task error.", e);
        }
    }

    private boolean success(List<SegmentPredicate> predicates) throws IOException {
        if (CollectionUtils.isEmpty(predicates)) {
            return true;
        }
        for (SegmentPredicate segPredicate : predicates) {
            Predicator predicate = PredicatorFactory.newPredicateInstance(segPredicate);
            if (!predicate.predicate()) {
                return false;
            }
        }
        return true;
    }


    private void initParam(JobDetail jd) throws IOException, SchedulerException {
        mPredicts = new ArrayList<>();
        livyUri = livyConfProps.getProperty("livy.uri");
        measure = JsonUtil.toEntity(jd.getJobDataMap().getString(MEASURE_KEY), Measure.class);
        setPredicts(jd.getJobDataMap().getString(PREDICTS_KEY));
        setMeasureInstanceName(measure, jd);
    }

    private void setPredicts(String json) throws IOException {
        if (StringUtils.isEmpty(json)) {
            return;
        }
        List<Map> maps = JsonUtil.toEntity(json, new TypeReference<List<Map>>(){});
        for (Map<String, Object> map : maps) {
            SegmentPredicate sp = new SegmentPredicate();
            sp.setType((String) map.get("type"));
            sp.setConfigMap((Map<String, String>) map.get("config"));
            mPredicts.add(sp);
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

    private void setLivyConf() throws JsonProcessingException {
        setLivyParams();
        setLivyArgs();
        setLivyJars();
        setPropConf();
    }

    private void setLivyParams() {
        livyConf.setFile(livyConfProps.getProperty("sparkJob.file"));
        livyConf.setClassName(livyConfProps.getProperty("sparkJob.className"));
        livyConf.setName(livyConfProps.getProperty("sparkJob.name"));
        livyConf.setQueue(livyConfProps.getProperty("sparkJob.queue"));
        livyConf.setNumExecutors(Long.parseLong(livyConfProps.getProperty("sparkJob.numExecutors")));
        livyConf.setExecutorCores(Long.parseLong(livyConfProps.getProperty("sparkJob.executorCores")));
        livyConf.setDriverMemory(livyConfProps.getProperty("sparkJob.driverMemory"));
        livyConf.setExecutorMemory(livyConfProps.getProperty("sparkJob.executorMemory"));
        livyConf.setFiles(new ArrayList<>());
    }

    private void setLivyArgs() throws JsonProcessingException {
        List<String> args = new ArrayList<>();
        args.add(livyConfProps.getProperty("sparkJob.args_1"));
        String measureJson = JsonUtil.toJsonWithFormat(measure);
        // to fix livy bug: character ` will be ignored by livy
        String finalMeasureJson = escapeCharacter(measureJson, "\\`");
        LOGGER.info(finalMeasureJson);
        args.add(finalMeasureJson);
        args.add(livyConfProps.getProperty("sparkJob.args_3"));
        livyConf.setArgs(args);
    }

    private void setLivyJars() {
        String jarProp = livyConfProps.getProperty("sparkJob.jars");
        List<String> jars = Arrays.asList(jarProp.split(SPARK_JOB_JARS_SPLIT));
        livyConf.setJars(jars);
    }

    private void setPropConf() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.yarn.dist.files", livyConfProps.getProperty("spark.yarn.dist.files"));
        livyConf.setConf(conf);
    }

    private void saveJobInstance(String groupName, String jobName, String result) {
        TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {
        };
        try {
            Map<String, Object> resultMap = JsonUtil.toEntity(result, type);
            if (resultMap != null) {
                JobInstanceBean jobInstance = genJobInstance(groupName, jobName, resultMap);
                jobInstanceRepo.save(jobInstance);
            }
        } catch (IOException e) {
            LOGGER.error("jobInstance jsonStr convert to map failed. {}", e.getMessage());
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Livy status is illegal. {}", e.getMessage());
        }
    }

    private JobInstanceBean genJobInstance(String groupName, String jobName, Map<String, Object> resultMap) throws IllegalArgumentException {
        JobInstanceBean jobInstance = new JobInstanceBean();
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
