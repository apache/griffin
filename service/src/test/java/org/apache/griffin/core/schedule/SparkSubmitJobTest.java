package org.apache.griffin.core.schedule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.griffin.core.measure.DataConnector;
import org.apache.griffin.core.measure.EvaluateRule;
import org.apache.griffin.core.measure.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by xiangrchen on 5/8/17.
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = {"classpath:context.xml"})
public class SparkSubmitJobTest {

    private SparkSubmitJob ssj;

    @Autowired
    MeasureRepo measureRepo;

    @Before
    public void setUp() throws IOException {
        ssj=new SparkSubmitJob();
        ssj.measureRepo=mock(MeasureRepo.class);
    }

    @Test
    public void test_execute() throws JsonProcessingException {
        JobExecutionContext context=mock(JobExecutionContext.class);
        JobDetail jd = mock(JobDetail.class);
        when(context.getJobDetail()).thenReturn(jd);

        JobDataMap jdmap = mock(JobDataMap.class);
        when(jd.getJobDataMap()).thenReturn(jdmap);

        when(jdmap.getString("measure")).thenReturn("bevssoj");
        when(jdmap.getString("sourcePat")).thenReturn("YYYYMMDD-HH");
        when(jdmap.getString("targetPat")).thenReturn("YYYYMMDD-HH");
        when(jdmap.getString("dataStartTimestamp")).thenReturn("1460174400000");
        when(jdmap.getString("lastTime")).thenReturn("");
        when(jdmap.getString("periodTime")).thenReturn("10");

        HashMap<String,String> configMap1=new HashMap<>();
        configMap1.put("database","default");
        configMap1.put("table.name","test_data_src");
        HashMap<String,String> configMap2=new HashMap<>();
        configMap2.put("database","default");
        configMap2.put("table.name","test_data_tgt");
        String configJson1 = new ObjectMapper().writeValueAsString(configMap1);
        String configJson2 = new ObjectMapper().writeValueAsString(configMap2);
        DataConnector source = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);
        String rules = "$source.uage > 100 AND $source.uid = $target.uid AND $source.uage + 12 = $target.uage + 10 + 2 AND $source.udes + 11 = $target.udes + 1 + 1";
        EvaluateRule eRule = new EvaluateRule(1,rules);
        Measure measure = new Measure("bevssoj","bevssoj description", Measure.MearuseType.accuracy, "bullyeye", source, target, eRule,"test1");

        when(ssj.measureRepo.findByName("bevssoj")).thenReturn(measure);
//        ssj.execute(context);

        RestTemplate restTemplate =mock(RestTemplate.class);
//        String uri="http://10.9.246.187:8998/batches";
        String uri="";
        SparkJobDO sparkJobDO=mock(SparkJobDO.class);
        when(restTemplate.postForObject(uri, sparkJobDO, String.class)).thenReturn(null);


        long currentSystemTimestamp=System.currentTimeMillis();
        long currentTimstamp = ssj.setCurrentTimestamp(currentSystemTimestamp);

//        verify(ssj.measureRepo).findByName("bevssoj");
//        verify(jdmap,atLeast(2)).put("lastTime",currentTimstamp+"");
    }

    @Test
    public void test_genPartitions(){
        String[] patternItemSet={"YYYYMMDD","HH"};
        String[] partitionItemSet={"date","hour"};
        long timestamp=1460174400000l;
        Map<String,String> par=ssj.genPartitions(patternItemSet,partitionItemSet,timestamp);
        Map<String,String> verifyMap=new HashMap<>();
        verifyMap.put("date","20160409");
        verifyMap.put("hour","12");
        assertEquals(verifyMap,par);
    }

   @Test
   public void test_setDataConnectorPartitions(){
       DataConnector dc=mock(DataConnector.class);
       String[] patternItemSet={"YYYYMMDD","HH"};
       String[] partitionItemSet={"date","hour"};
       long timestamp=1460174400000l;
       ssj.setDataConnectorPartitions(dc,patternItemSet,partitionItemSet,timestamp);
//       doNothing().when(ssj).setDataConnectorPartitions(dataConnector,patternItemSet,partitionItemSet,timestamp);
       Map<String,String> map=new HashMap<>();
       map.put("partitions","date=20160409, hour=12");
       try {
           verify(dc).setConfig(map);
       } catch (JsonProcessingException e) {
           e.printStackTrace();
       }
   }

    @Test
    public void test_setCurrentTimestamp(){
        long timestamp=System.currentTimeMillis();
        ssj.eachJoblastTimestamp="";
        System.out.println(ssj.setCurrentTimestamp(timestamp));
        ssj.eachJoblastTimestamp="1494297256667";
        ssj.periodTime="10";
        System.out.println(ssj.setCurrentTimestamp(timestamp));
    }

    @Test
    public void test_setSparkJobDO(){
        ssj=mock(SparkSubmitJob.class);
        doNothing().when(ssj).setSparkJobDO();
    }

    @Test
    public void test_getsparkJobProperties(){
        ssj=mock(SparkSubmitJob.class);
        try {
            when(ssj.getsparkJobProperties()).thenReturn(null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
