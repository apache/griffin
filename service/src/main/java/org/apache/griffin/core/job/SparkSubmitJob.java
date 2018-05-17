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
import org.apache.griffin.core.job.entity.JobInstanceBean;
import org.apache.griffin.core.job.entity.LivyConf;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.job.entity.SegmentPredicate;
import org.apache.griffin.core.job.factory.PredicatorFactory;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.util.JsonUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;

import static org.apache.griffin.core.job.JobInstance.*;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class SparkSubmitJob implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSubmitJob.class);
    private static final String SPARK_JOB_JARS_SPLIT = ";";

    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    @Qualifier("livyConf")
    private Properties livyConfProps;
    @Autowired
    private JobServiceImpl jobService;

    private GriffinMeasure measure;
    private String livyUri;
    private List<SegmentPredicate> mPredicates;
    private JobInstanceBean jobInstance;
    private RestTemplate restTemplate = new RestTemplate();
    private LivyConf livyConf = new LivyConf();

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
            LOGGER.error("Post spark task error.", e);
        }
    }

    private void updateJobInstanceState(JobExecutionContext context) throws IOException {
        SimpleTrigger simpleTrigger = (SimpleTrigger) context.getTrigger();
        int repeatCount = simpleTrigger.getRepeatCount();
        int fireCount = simpleTrigger.getTimesTriggered();
        if (fireCount > repeatCount) {
            saveJobInstance(null, LivySessionStates.State.not_found);
        }
    }

    private String post2Livy() {
        String result;
        try {
            result = restTemplate.postForObject(livyUri, livyConf, String.class);
            LOGGER.info(result);
        } catch (Exception e) {
            LOGGER.error("Post to livy error. {}", e.getMessage());
            result = null;
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
        livyUri = livyConfProps.getProperty("livy.uri");
        jobInstance = jobInstanceRepo.findByPredicateName(jd.getJobDataMap().getString(PREDICATE_JOB_NAME));
        measure = JsonUtil.toEntity(jd.getJobDataMap().getString(MEASURE_KEY), GriffinMeasure.class);
        setPredicates(jd.getJobDataMap().getString(PREDICATES_KEY));
        setMeasureInstanceName(measure, jd);
    }

    @SuppressWarnings("unchecked")
    private void setPredicates(String json) throws IOException {
        if (StringUtils.isEmpty(json)) {
            return;
        }
        List<Map<String, Object>> maps = JsonUtil.toEntity(json, new TypeReference<List<Map>>() {
        });
        assert maps != null;
        for (Map<String, Object> map : maps) {
            SegmentPredicate sp = new SegmentPredicate();
            sp.setType((String) map.get("type"));
            sp.setConfigMap((Map<String, String>) map.get("config"));
            mPredicates.add(sp);
        }
    }

    private void setMeasureInstanceName(GriffinMeasure measure, JobDetail jd) {
        // in order to keep metric name unique, we set job name as measure name at present
        measure.setName(jd.getJobDataMap().getString(JOB_NAME));
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
        String v = livyConfProps.getProperty("spark.yarn.dist.files");
        if (!StringUtils.isEmpty(v)) {
            conf.put("spark.yarn.dist.files", v);
            livyConf.setConf(conf);
        }
    }

    private void saveJobInstance(JobDetail jd) throws SchedulerException, IOException {
        String result = post2Livy();
        if (result != null) {
            String group = jd.getKey().getGroup();
            String name = jd.getKey().getName();
            jobService.pauseJob(group, name);
            LOGGER.info("Delete predicate job({},{}) success.", group, name);
        }
        saveJobInstance(result, LivySessionStates.State.found);
    }

    private void saveJobInstance(String result, LivySessionStates.State state) throws IOException {
        TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {
        };
        Map<String, Object> resultMap = null;
        if (result != null) {
            resultMap = JsonUtil.toEntity(result, type);
        }
        setJobInstance(resultMap, state);
        jobInstanceRepo.save(jobInstance);
    }

    private void setJobInstance(Map<String, Object> resultMap, LivySessionStates.State state) {
        jobInstance.setState(state);
        jobInstance.setDeleted(true);
        if (resultMap == null) {
            return;
        }
        if (resultMap.get("state") != null) {
            jobInstance.setState(LivySessionStates.State.valueOf(resultMap.get("state").toString()));
        }
        if (resultMap.get("id") != null) {
            jobInstance.setSessionId(Long.parseLong(resultMap.get("id").toString()));
        }
        if (resultMap.get("appId") != null) {
            jobInstance.setAppId(resultMap.get("appId").toString());
        }
    }

}
