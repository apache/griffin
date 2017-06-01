package org.apache.griffin.core.schedule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.Gson;
import org.apache.griffin.core.measure.DataConnector;
import org.apache.griffin.core.measure.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.schedule.Repo.ScheduleStateRepo;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class SparkSubmitJob implements Job {
    Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    MeasureRepo measureRepo;

    @Autowired
    ScheduleStateRepo scheduleStateRepo;

    Properties props;

    String patItem;
    /**
     * partitionItemSet
     * for example
     * partitionItemSet like "date","hour",...
     */
    String[] partitionItemSet;
    /**
     * sourcePatternItemSet targetPatternItemSet
     * for example
     * sourcePatternItemSet like "YYYYMMDD","HH",...
     */
    String[] sourcePatternItemSet;
    String[] targetPatternItemSet;
    /**
     * sourcePatternPartitionSizeMin is the min between partitionItemSet.length and sourcePatternItemSet.length
     */
    int sourcePatternPartitionSizeMin = 0;
    int targetPatternPartitionSizeMin = 0;

    Measure measure;
    String sourcePattern;
    String targetPattern;
    String dataStartTimestamp;
    String eachJoblastTimestamp;
    String periodTime;

    String uri;
    SparkJobDO sparkJobDO = new SparkJobDO();

    public SparkSubmitJob() throws IOException {
        try {
            props = getsparkJobProperties();
            patItem = props.getProperty("sparkJob.dateAndHour");
            partitionItemSet = patItem.split(",");
            uri = props.getProperty("sparkJob.uri");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(JobExecutionContext context) {
        JobDetail jd = context.getJobDetail();
        String groupName=jd.getJobDataMap().getString("groupName");
        String jobName=jd.getJobDataMap().getString("jobName");
        String measureName = jd.getJobDataMap().getString("measure");
        measure = measureRepo.findByName(measureName);
        if (measure==null) {
            logger.info(measureName + " is not find!");
            return;
        }
        sourcePattern = jd.getJobDataMap().getString("sourcePat");
        targetPattern = jd.getJobDataMap().getString("targetPat");
        dataStartTimestamp = jd.getJobDataMap().getString("dataStartTimestamp");
        eachJoblastTimestamp = jd.getJobDataMap().getString("lastTime");
        logger.info(eachJoblastTimestamp);
        periodTime = jd.getJobDataMap().getString("periodTime");
        //prepare current system timestamp
        long currentSystemTimestamp = System.currentTimeMillis();

        if (sourcePattern != null && !sourcePattern.isEmpty()) {
            sourcePatternItemSet = sourcePattern.split("-");
            long currentTimstamp = setCurrentTimestamp(currentSystemTimestamp);
            setDataConnectorPartitions(measure.getSource(), sourcePatternItemSet, partitionItemSet, currentTimstamp);
            jd.getJobDataMap().put("lastTime", currentTimstamp + "");
        }
        if (targetPattern != null && !targetPattern.equals("")) {
            targetPatternItemSet = targetPattern.split("-");
            long currentTimstamp = setCurrentTimestamp(currentSystemTimestamp);
            setDataConnectorPartitions(measure.getTarget(), targetPatternItemSet, partitionItemSet, currentTimstamp);
            jd.getJobDataMap().put("lastTime", currentTimstamp + "");
        }
        //final String uri = "http://10.9.246.187:8998/batches";
        RestTemplate restTemplate = new RestTemplate();
        setSparkJobDO();
        String result = restTemplate.postForObject(uri, sparkJobDO, String.class);
        logger.info(result);
        ScheduleResult scheduleResult=new ScheduleResult();
        Gson gson=new Gson();
        try {
            scheduleResult=gson.fromJson(result,ScheduleResult.class);
        }catch (Exception e){
            logger.info("scheduleResult covert error!"+e);
        }
        ScheduleState scheduleState=new ScheduleState(groupName,jobName,scheduleResult.getId(),scheduleResult.getState(),scheduleResult.getAppId(),System.currentTimeMillis());
        scheduleStateRepo.save(scheduleState);
        //	{"file": "/exe/griffin-measure-batch-0.0.1-SNAPSHOT.jar", "className": "org.apache.griffin.measure.batch.Application", "args": ["/benchmark/test/env.json", "/benchmark/test/config-rdm.json"], "name": "griffin-livy", "queue": "default", "numExecutors": 2, "executorCores": 4, "driverMemory": "2g", "executorMemory": "2g", "conf": {"spark.jars.packages": "com.databricks:spark-avro_2.10:2.0.1"}, "jars": ["/livy/datanucleus-api-jdo-3.2.6.jar", "/livy/datanucleus-core-3.2.10.jar", "/livy/datanucleus-rdbms-3.2.9.jar"], "files": ["/livy/hive-site.xml"]}' -H "Content-Type: application/json"
    }

    public Map<String, String> genPartitions(String[] patternItemSet, String[] partitionItemSet, long timestamp) {
        int comparableSizeMin=Math.min(patternItemSet.length,partitionItemSet.length);
        Map<String, String> res = new HashMap<>();
        for (int i = 0; i < comparableSizeMin; i++) {
            /**
             * in order to get a standard date like 20170427 01
             */
            String pattrn = patternItemSet[i].replace("mm", "MM");
            pattrn = pattrn.replace("DD", "dd");
            pattrn = pattrn.replace("hh", "HH");
            SimpleDateFormat sdf = new SimpleDateFormat(pattrn);
            res.put(partitionItemSet[i], sdf.format(new Date(timestamp)));
        }
        return res;
    }

    /**
     * putDataConnectorPartitions
     *
     * @param dc
     * @param patternItemSet
     * @param partitionItemSet
     * @param timestamp
     * @return
     */
    public void setDataConnectorPartitions(DataConnector dc, String[] patternItemSet, String[] partitionItemSet, long timestamp) {
        Map<String, String> partitionItemMap = genPartitions(patternItemSet, partitionItemSet, timestamp);
        String partitions = partitionItemMap.toString().substring(1, partitionItemMap.toString().length() - 1);

        Map<String, String> configMap = dc.getConfigInMaps();
        configMap.put("partitions", partitions);
        try {
            dc.setConfig(configMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public long setCurrentTimestamp(long currentSystemTimestamp) {
        long currentTimstamp;
        if (eachJoblastTimestamp != null && !eachJoblastTimestamp.equals("")) {
            currentTimstamp = Long.parseLong(eachJoblastTimestamp) + Integer.parseInt(periodTime) * 1000;
        } else {
            if (dataStartTimestamp != null && !dataStartTimestamp.equals("")) {
                currentTimstamp = Long.parseLong(dataStartTimestamp);
            } else {
                currentTimstamp = currentSystemTimestamp;
            }
        }
        return currentTimstamp;
    }

    public void setSparkJobDO() {
        sparkJobDO.setFile(props.getProperty("sparkJob.file"));
        sparkJobDO.setClassName(props.getProperty("sparkJob.className"));

        List<String> args = new ArrayList<>();
        args.add(props.getProperty("sparkJob.args_1"));
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String measureJson = "";
        try {
            measureJson = ow.writeValueAsString(measure);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        args.add(measureJson);  //partition
        args.add(props.getProperty("sparkJob.args_3"));
        sparkJobDO.setArgs(args);

        sparkJobDO.setName(props.getProperty("sparkJob.name"));
        sparkJobDO.setQueue(props.getProperty("sparkJob.queue"));
        sparkJobDO.setNumExecutors(Long.parseLong(props.getProperty("sparkJob.numExecutors")));
        sparkJobDO.setExecutorCores(Long.parseLong(props.getProperty("sparkJob.executorCores")));
        sparkJobDO.setDriverMemory(props.getProperty("sparkJob.driverMemory"));
        sparkJobDO.setExecutorMemory(props.getProperty("sparkJob.executorMemory"));

        Conf conf = new Conf();
        conf.setSpark_jars_packages(props.getProperty("sparkJob.spark.jars.packages"));
        sparkJobDO.setConf(conf);

        List<String> jars = new ArrayList<>();
        jars.add(props.getProperty("sparkJob.jars_1"));
        jars.add(props.getProperty("sparkJob.jars_2"));
        jars.add(props.getProperty("sparkJob.jars_3"));
        sparkJobDO.setJars(jars);

        List<String> files = new ArrayList<>();
//        files.add(props.getProperty("sparkJob.files_1"));

        sparkJobDO.setFiles(files);
    }

    public Properties getsparkJobProperties() throws IOException {
        InputStream inputStream = null;
        Properties prop = new Properties();
        ;
        try {
            String propFileName = "sparkJob.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }
        return prop;
    }
}
