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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.job.entity.SparkJobDO;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinUtil;
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
    private String[] sourcePatternItems,targetPatternItems;

    private Measure measure;
    private String sourcePattern,targetPattern;
    private String blockStartTimestamp,lastBlockStartTimestamp;
    private String interval;
    private String uri;
    private RestTemplate restTemplate = new RestTemplate();
    private SparkJobDO sparkJobDO = new SparkJobDO();

    public SparkSubmitJob() {
    }

    /**
     * execute method is used to submit sparkJobDO to Livy.
     * @param context
     */
    @Override
    public void execute(JobExecutionContext context) {
        JobDetail jd = context.getJobDetail();
        String groupName=jd.getJobDataMap().getString("groupName");
        String jobName=jd.getJobDataMap().getString("jobName");
        init(jd);
        //prepare current system timestamp
        long currentblockStartTimestamp = setCurrentblockStartTimestamp(System.currentTimeMillis());
        LOGGER.info("currentblockStartTimestamp: "+currentblockStartTimestamp);
        if (StringUtils.isNotEmpty(sourcePattern)) {
            sourcePatternItems = sourcePattern.split("-");
            setDataConnectorPartitions(measure.getSource(), sourcePatternItems, partitionItems, currentblockStartTimestamp);
        }
        if (StringUtils.isNotEmpty(targetPattern)) {
            targetPatternItems = targetPattern.split("-");
            setDataConnectorPartitions(measure.getTarget(), targetPatternItems, partitionItems, currentblockStartTimestamp);
        }
        jd.getJobDataMap().put("lastBlockStartTimestamp", currentblockStartTimestamp + "");
        setSparkJobDO();
        String result = restTemplate.postForObject(uri, sparkJobDO, String.class);
        LOGGER.info(result);
        saveJobInstance(groupName,jobName,result);
    }

    public void init(JobDetail jd){
        //jd.getJobDataMap().getString()
        /**
         * the field measureId is generated from `setJobData` in `JobServiceImpl`
         */
        String measureId = jd.getJobDataMap().getString("measureId");
        measure = measureRepo.findOne(Long.valueOf(measureId));
        if (measure==null) {
            LOGGER.error("Measure with id " + measureId + " is not find!");
            //if return here, livy uri won't be set, and will keep null for all measures even they are not null
        }
        String partitionItemstr = sparkJobProps.getProperty("sparkJob.dateAndHour");
        partitionItems = partitionItemstr.split(",");
        uri = sparkJobProps.getProperty("livy.uri");
        sourcePattern = jd.getJobDataMap().getString("sourcePattern");
        targetPattern = jd.getJobDataMap().getString("targetPattern");
        blockStartTimestamp = jd.getJobDataMap().getString("blockStartTimestamp");
        lastBlockStartTimestamp = jd.getJobDataMap().getString("lastBlockStartTimestamp");
        LOGGER.info("lastBlockStartTimestamp:"+lastBlockStartTimestamp);
        interval = jd.getJobDataMap().getString("interval");
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
            LOGGER.error(""+e);
        }
    }

    public Map<String, String> genPartitionMap(String[] patternItemSet, String[] partitionItems, long timestamp) {
        /**
         * patternItemSet:{YYYYMMdd,HH}
         * partitionItems:{dt,hour}
         * partitionItemMap:{dt=20170804,hour=09}
         */
        int comparableSizeMin=Math.min(patternItemSet.length,partitionItems.length);
        Map<String, String> partitionItemMap = new HashMap<>();
        for (int i = 0; i < comparableSizeMin; i++) {
            /**
             * in order to get a standard date like 20170427 01 (YYYYMMdd-HH)
             */
            String pattrn = patternItemSet[i].replace("mm", "MM");
            pattrn = pattrn.replace("DD", "dd");
            pattrn = pattrn.replace("hh", "HH");
            SimpleDateFormat sdf = new SimpleDateFormat(pattrn);
            partitionItemMap.put(partitionItems[i], sdf.format(new Date(timestamp)));
        }
        return partitionItemMap;
    }


    public long setCurrentblockStartTimestamp(long currentSystemTimestamp) {
        long currentblockStartTimestamp=0;
        if (StringUtils.isNotEmpty(lastBlockStartTimestamp)) {
            try {
                currentblockStartTimestamp = Long.parseLong(lastBlockStartTimestamp) + Integer.parseInt(interval) * 1000;
            }catch (Exception e){
                LOGGER.info("lastBlockStartTimestamp or interval format problem! "+e);
            }
        } else {
            if (StringUtils.isNotEmpty(blockStartTimestamp)) {
                try{
                    currentblockStartTimestamp = Long.parseLong(blockStartTimestamp);
                }catch (Exception e){
                    LOGGER.info("blockStartTimestamp format problem! "+e);
                }
            } else {
                currentblockStartTimestamp = currentSystemTimestamp;
            }
        }
        return currentblockStartTimestamp;
    }

    public void setSparkJobDO() {
        sparkJobDO.setFile(sparkJobProps.getProperty("sparkJob.file"));
        sparkJobDO.setClassName(sparkJobProps.getProperty("sparkJob.className"));

        List<String> args = new ArrayList<String>();
        args.add(sparkJobProps.getProperty("sparkJob.args_1"));
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String measureJson = "";
        try {
            measureJson = ow.writeValueAsString(measure);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        args.add(measureJson);  //partition
        args.add(sparkJobProps.getProperty("sparkJob.args_3"));
        sparkJobDO.setArgs(args);

        sparkJobDO.setName(sparkJobProps.getProperty("sparkJob.name"));
        sparkJobDO.setQueue(sparkJobProps.getProperty("sparkJob.queue"));
        sparkJobDO.setNumExecutors(Long.parseLong(sparkJobProps.getProperty("sparkJob.numExecutors")));
        sparkJobDO.setExecutorCores(Long.parseLong(sparkJobProps.getProperty("sparkJob.executorCores")));
        sparkJobDO.setDriverMemory(sparkJobProps.getProperty("sparkJob.driverMemory"));
        sparkJobDO.setExecutorMemory(sparkJobProps.getProperty("sparkJob.executorMemory"));

        Map<String,String> conf=new HashMap<String,String>();
        conf.put("spark.jars.packages",sparkJobProps.getProperty("sparkJob.spark.jars.packages"));
        sparkJobDO.setConf(conf);

        List<String> jars = new ArrayList<>();
        jars.add(sparkJobProps.getProperty("sparkJob.jars_1"));
        jars.add(sparkJobProps.getProperty("sparkJob.jars_2"));
        jars.add(sparkJobProps.getProperty("sparkJob.jars_3"));
        sparkJobDO.setJars(jars);

        List<String> files = new ArrayList<>();
        sparkJobDO.setFiles(files);
    }

    public void saveJobInstance(String groupName,String jobName,String result){
        //save JobInstance info into DataBase
        Map<String,Object> resultMap=new HashMap<String,Object>();
        TypeReference<HashMap<String,Object>> type=new TypeReference<HashMap<String,Object>>(){};
        try {
            resultMap= GriffinUtil.toEntity(result,type);
        } catch (IOException e) {
            LOGGER.error("jobInstance jsonStr convert to map failed. "+e);
        }
        JobInstance jobInstance=new JobInstance();
        if(resultMap!=null) {
            jobInstance.setGroupName(groupName);
            jobInstance.setJobName(jobName);
            try {
                jobInstance.setSessionId(Integer.parseInt(resultMap.get("id").toString()));
                jobInstance.setState(LivySessionStates.State.valueOf(resultMap.get("state").toString()));
                jobInstance.setAppId(resultMap.get("appId").toString());
            }catch (Exception e){
                LOGGER.warn("jobInstance has null field. "+e);
            }
            jobInstance.setTimestamp(System.currentTimeMillis());
            jobInstanceRepo.save(jobInstance);
        }
    }
}
