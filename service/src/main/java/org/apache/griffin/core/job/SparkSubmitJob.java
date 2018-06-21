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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import org.apache.griffin.core.job.entity.JobInstanceBean;
import org.apache.griffin.core.job.entity.SegmentPredicate;
import org.apache.griffin.core.job.factory.PredicatorFactory;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType;
import org.apache.griffin.core.util.JsonUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.griffin.core.config.EnvConfig.ENV_BATCH;
import static org.apache.griffin.core.config.EnvConfig.ENV_STREAMING;
import static org.apache.griffin.core.config.PropertiesConfig.livyConfMap;
import static org.apache.griffin.core.job.JobInstance.*;
import static org.apache.griffin.core.job.entity.LivySessionStates.State;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.FOUND;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.NOT_FOUND;
import static org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType.BATCH;
import static org.apache.griffin.core.util.JsonUtil.toEntity;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
@Component
public class SparkSubmitJob implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSubmitJob.class);

    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    private BatchJobOperatorImpl batchJobOp;
    @Autowired
    private Environment env;

    private GriffinMeasure measure;
    private String livyUri;
    private List<SegmentPredicate> mPredicates;
    private JobInstanceBean jobInstance;
    private RestTemplate restTemplate = new RestTemplate();

    @Override
    public void execute(JobExecutionContext context) {
        JobDetail jd = context.getJobDetail();
        try {
            initParam(jd);
            setLivyConf();
            if (!success(mPredicates)) {
                updateJobInstanceState(context);
                return;
            }
            saveJobInstance(jd);
        } catch (Exception e) {
            LOGGER.error("Post spark task ERROR.", e);
        }
    }

    private void updateJobInstanceState(JobExecutionContext context) throws IOException {
        SimpleTrigger simpleTrigger = (SimpleTrigger) context.getTrigger();
        int repeatCount = simpleTrigger.getRepeatCount();
        int fireCount = simpleTrigger.getTimesTriggered();
        if (fireCount > repeatCount) {
            saveJobInstance(null, NOT_FOUND);
        }
    }

    private String post2Livy() {
        String result = null;
        try {
            result = restTemplate.postForObject(livyUri, livyConfMap, String.class);
            LOGGER.info(result);
        } catch (HttpClientErrorException e) {
            LOGGER.error("Post to livy ERROR. \n {} {}", e.getMessage(), e.getResponseBodyAsString());
        } catch (Exception e) {
            LOGGER.error("Post to livy ERROR. {}", e.getMessage());
        }
        return result;
    }

    private boolean success(List<SegmentPredicate> predicates) {
        if (CollectionUtils.isEmpty(predicates)) {
            return true;
        }
        for (SegmentPredicate segPredicate : predicates) {
            Predicator predicator = PredicatorFactory.newPredicateInstance(segPredicate);
            try {
                if (predicator != null && !predicator.predicate()) {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }

        }
        return true;
    }

    private void initParam(JobDetail jd) throws IOException {
        mPredicates = new ArrayList<>();
        jobInstance = jobInstanceRepo.findByPredicateName(jd.getJobDataMap().getString(PREDICATE_JOB_NAME));
        measure = toEntity(jd.getJobDataMap().getString(MEASURE_KEY), GriffinMeasure.class);
        livyUri = env.getProperty("livy.uri");
        setPredicates(jd.getJobDataMap().getString(PREDICATES_KEY));
        // in order to keep metric name unique, we set job name as measure name at present
        measure.setName(jd.getJobDataMap().getString(JOB_NAME));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void setPredicates(String json) throws IOException {
        if (StringUtils.isEmpty(json)) {
            return;
        }
        List<Map<String, Object>> maps = toEntity(json, new TypeReference<List<Map>>() {
        });
        for (Map<String, Object> map : maps) {
            SegmentPredicate sp = new SegmentPredicate();
            sp.setType((String) map.get("type"));
            sp.setConfigMap((Map<String, Object>) map.get("config"));
            mPredicates.add(sp);
        }
    }

    private String escapeCharacter(String str, String regex) {
        if (StringUtils.isEmpty(str)) {
            return str;
        }
        String escapeCh = "\\" + regex;
        return str.replaceAll(regex, escapeCh);
    }

    private String genEnv() {
        ProcessType type = measure.getProcessType();
        String env = type == BATCH ? ENV_BATCH : ENV_STREAMING;
        return env.replaceAll("\\$\\{JOB_NAME}", measure.getName());
    }

    private void setLivyConf() throws IOException {
        setLivyArgs();
    }

    private void setLivyArgs() throws IOException {
        List<String> args = new ArrayList<>();
        args.add(genEnv());
        String measureJson = JsonUtil.toJsonWithFormat(measure);
        // to fix livy bug: character ` will be ignored by livy
        String finalMeasureJson = escapeCharacter(measureJson, "\\`");
        LOGGER.info(finalMeasureJson);
        args.add(finalMeasureJson);
        args.add("raw,raw");
        livyConfMap.put("args", args);
    }


    private void saveJobInstance(JobDetail jd) throws SchedulerException, IOException {
        // If result is null, it may livy uri is wrong or livy parameter is wrong.
        String result = post2Livy();
        String group = jd.getKey().getGroup();
        String name = jd.getKey().getName();
        batchJobOp.deleteJob(group, name);
        LOGGER.info("Delete predicate job({},{}) SUCCESS.", group, name);
        saveJobInstance(result, FOUND);
    }

    private void saveJobInstance(String result, State state) throws IOException {
        TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {
        };
        Map<String, Object> resultMap = null;
        if (result != null) {
            resultMap = toEntity(result, type);
        }
        setJobInstance(resultMap, state);
        jobInstanceRepo.save(jobInstance);
    }

    private void setJobInstance(Map<String, Object> resultMap, State state) {
        jobInstance.setState(state);
        jobInstance.setPredicateDeleted(true);
        if (resultMap != null) {
            Object status = resultMap.get("state");
            Object id = resultMap.get("id");
            Object appId = resultMap.get("appId");
            jobInstance.setState(status == null ? null : State.valueOf(status.toString().toUpperCase()));
            jobInstance.setSessionId(id == null ? null : Long.parseLong(id.toString()));
            jobInstance.setAppId(appId == null ? null : appId.toString());
        }
    }

}
