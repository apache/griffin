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
import org.apache.griffin.core.job.entity.SparkJobDO;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.JsonUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class SparkSubmitJob implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSubmitJob.class);

    @Autowired
    private MeasureRepo measureRepo;
    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    private Properties sparkJobProps;

    /**
     * partitionItems
     * for example
     * partitionItems like "date","hour",...
     */
    private String[] partitionItems;
    /**
     * sourcePatternItems targetPatternItems
     * for example
     * sourcePatternItems or targetPatternItems is like "YYYYMMDD","HH",...
     */
    private String[] sourcePatternItems, targetPatternItems;

    private Measure measure;
    private String sourcePattern, targetPattern;
    private String blockStartTimestamp, lastBlockStartTimestamp;
    private String interval;
    private String uri;
    private RestTemplate restTemplate = new RestTemplate();
    private SparkJobDO sparkJobDO = new SparkJobDO();

    public SparkSubmitJob() {
    }

    /**
     * execute method is used to submit sparkJobDO to Livy.
     *
     * @param context Job execution context
     */
    @Override
    public void execute(JobExecutionContext context) {
        JobDetail jd = context.getJobDetail();
        String groupName = jd.getJobDataMap().getString("groupName");
        String jobName = jd.getJobDataMap().getString("jobName");
        initParam(jd);
        //prepare current system timestamp
        long currentBlockStartTimestamp = setCurrentBlockStartTimestamp(System.currentTimeMillis());
        LOGGER.info("currentBlockStartTimestamp: {}", currentBlockStartTimestamp);
        try {
            if (StringUtils.isNotEmpty(sourcePattern)) {
                setAllDataConnectorPartitions(measure.getDataSources(), sourcePattern.split("-"), partitionItems, "source", currentBlockStartTimestamp);
            }
            if (StringUtils.isNotEmpty(targetPattern)) {
                setAllDataConnectorPartitions(measure.getDataSources(), targetPattern.split("-"), partitionItems, "target", currentBlockStartTimestamp);
            }
        } catch (Exception e) {
            LOGGER.error("Can not execute job.Set partitions error. {}", e.getMessage());
            return;
        }
        jd.getJobDataMap().put("lastBlockStartTimestamp", currentBlockStartTimestamp + "");
        setSparkJobDO();
        String result;
        try {
            result = restTemplate.postForObject(uri, sparkJobDO, String.class);
        } catch (Exception e) {
            LOGGER.error("Post spark task error. {}", e.getMessage());
            return;
        }
        LOGGER.info(result);
        saveJobInstance(groupName, jobName, result);
    }

    private void initParam(JobDetail jd) {
        /**
         * the field measureId is generated from `setJobData` in `JobServiceImpl`
         */
        String measureId = jd.getJobDataMap().getString("measureId");
        measure = measureRepo.findOne(Long.valueOf(measureId));
        if (measure == null) {
            LOGGER.error("Measure with id {} is not find!", measureId);
            return;
        }
        setMeasureInstanceName(measure, jd);
        partitionItems = sparkJobProps.getProperty("sparkJob.dateAndHour").split(",");
        uri = sparkJobProps.getProperty("livy.uri");
        sourcePattern = jd.getJobDataMap().getString("sourcePattern");
        targetPattern = jd.getJobDataMap().getString("targetPattern");
        blockStartTimestamp = jd.getJobDataMap().getString("blockStartTimestamp");
        lastBlockStartTimestamp = jd.getJobDataMap().getString("lastBlockStartTimestamp");
        LOGGER.info("lastBlockStartTimestamp:{}", lastBlockStartTimestamp);
        interval = jd.getJobDataMap().getString("interval");
    }

    private void setMeasureInstanceName(Measure measure, JobDetail jd) {
        // in order to keep metric name unique, we set measure name as jobName at present
        measure.setName(jd.getJobDataMap().getString("jobName"));
    }

    private void setAllDataConnectorPartitions(List<DataSource> sources, String[] patternItemSet, String[] partitionItems, String sourceName, long timestamp) {
        if (sources == null) {
            return;
        }
        for (DataSource dataSource : sources) {
            setDataSourcePartitions(dataSource, patternItemSet, partitionItems, sourceName, timestamp);
        }
    }

    private void setDataSourcePartitions(DataSource dataSource, String[] patternItemSet, String[] partitionItems, String sourceName, long timestamp) {
        String name = dataSource.getName();
        for (DataConnector dataConnector : dataSource.getConnectors()) {
            if (sourceName.equals(name)) {
                setDataConnectorPartitions(dataConnector, patternItemSet, partitionItems, timestamp);
            }
        }
    }

    private void setDataConnectorPartitions(DataConnector dc, String[] patternItemSet, String[] partitionItems, long timestamp) {
        Map<String, String> partitionItemMap = genPartitionMap(patternItemSet, partitionItems, timestamp);
        /**
         * partitions must be a string like: "dt=20170301, hour=12"
         * partitionItemMap.toString() is like "{dt=20170301, hour=12}"
         */
        String partitions = partitionItemMap.toString().substring(1, partitionItemMap.toString().length() - 1);
        Map<String, String> configMap = dc.getConfigInMaps();
        //config should not be null
        configMap.put("partitions", partitions);
        try {
            dc.setConfig(configMap);
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getMessage());
        }
    }


    private Map<String, String> genPartitionMap(String[] patternItemSet, String[] partitionItems, long timestamp) {
        /**
         * patternItemSet:{YYYYMMdd,HH}
         * partitionItems:{dt,hour}
         * partitionItemMap:{dt=20170804,hour=09}
         */
        int comparableSizeMin = Math.min(patternItemSet.length, partitionItems.length);
        Map<String, String> partitionItemMap = new HashMap<>();
        for (int i = 0; i < comparableSizeMin; i++) {
            /**
             * in order to get a standard date like 20170427 01 (YYYYMMdd-HH)
             */
            String pattern = patternItemSet[i].replace("mm", "MM");
            pattern = pattern.replace("DD", "dd");
            pattern = pattern.replace("hh", "HH");
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            partitionItemMap.put(partitionItems[i], sdf.format(new Date(timestamp)));
        }
        return partitionItemMap;
    }


    private long setCurrentBlockStartTimestamp(long currentSystemTimestamp) {
        long currentBlockStartTimestamp = 0;
        if (StringUtils.isNotEmpty(lastBlockStartTimestamp)) {
            try {
                currentBlockStartTimestamp = Long.parseLong(lastBlockStartTimestamp) + Integer.parseInt(interval) * 1000;
            } catch (Exception e) {
                LOGGER.info("lastBlockStartTimestamp or interval format problem! {}", e.getMessage());
            }
        } else {
            if (StringUtils.isNotEmpty(blockStartTimestamp)) {
                try {
                    currentBlockStartTimestamp = Long.parseLong(blockStartTimestamp);
                } catch (Exception e) {
                    LOGGER.info("blockStartTimestamp format problem! {}", e.getMessage());
                }
            } else {
                currentBlockStartTimestamp = currentSystemTimestamp;
            }
        }
        return currentBlockStartTimestamp;
    }

    private void setSparkJobDO() {
        sparkJobDO.setFile(sparkJobProps.getProperty("sparkJob.file"));
        sparkJobDO.setClassName(sparkJobProps.getProperty("sparkJob.className"));

        List<String> args = new ArrayList<>();
        args.add(sparkJobProps.getProperty("sparkJob.args_1"));
        // measure
        String measureJson;
        measure.setTriggerTimeStamp(System.currentTimeMillis());
        measureJson = JsonUtil.toJsonWithFormat(measure);
        args.add(measureJson);
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

    public void saveJobInstance(String groupName, String jobName, String result) {
        TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {};
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

    private JobInstance genJobInstance(String groupName, String jobName, Map<String, Object> resultMap) throws IllegalArgumentException{
        JobInstance jobInstance = new JobInstance();
        jobInstance.setGroupName(groupName);
        jobInstance.setJobName(jobName);
        jobInstance.setTimestamp(System.currentTimeMillis());
        jobInstance.setSessionId(Integer.parseInt(resultMap.get("id").toString()));
        jobInstance.setState(LivySessionStates.State.valueOf(resultMap.get("state").toString()));
        if (resultMap.get("appId") != null) {
            jobInstance.setAppId(resultMap.get("appId").toString());
        }
        return jobInstance;
    }
}
