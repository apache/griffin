package org.apache.griffin.core;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.griffin.core.common.SimpleCORSFilter;
import org.apache.griffin.core.measure.DataConnector;
import org.apache.griffin.core.measure.DataConnector.ConnectorType;
import org.apache.griffin.core.measure.EvaluateRule;
import org.apache.griffin.core.measure.Measure;
import org.apache.griffin.core.measure.repo.DataConnectorRepo;
import org.apache.griffin.core.measure.repo.EvaluateRuleRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;

//import org.apache.griffin.core.measure.repo.ConnectorConfigRepo;

@SpringBootApplication
public class GriffinWebApplication implements CommandLineRunner{
    private static final Logger log = LoggerFactory.getLogger(GriffinWebApplication.class);
    public static void main(String[] args) {
        log.info("application start");
        SpringApplication.run(GriffinWebApplication.class, args);
    }

    @Autowired
    MeasureRepo measureRepo;
    @Autowired
    EvaluateRuleRepo evaluateRuleRepo;
    @Autowired
    DataConnectorRepo connectorRepo;



    public void run(String... strings) throws Exception {
        HashMap<String,String> configMap1=new HashMap<>();
        configMap1.put("database","default");
        configMap1.put("table.name","test_data_src");
        HashMap<String,String> configMap2=new HashMap<>();
        configMap2.put("database","default");
        configMap2.put("table.name","test_data_tgt");
        String configJson1 = new ObjectMapper().writeValueAsString(configMap1);
        String configJson2 = new ObjectMapper().writeValueAsString(configMap2);

        DataConnector source = new DataConnector(ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target = new DataConnector(ConnectorType.HIVE, "1.2", configJson2);

        String rules = "$source.uage > 100 AND $source.uid = $target.uid AND $source.uage + 12 = $target.uage + 10 + 2 AND $source.udes + 11 = $target.udes + 1 + 1";

        EvaluateRule eRule = new EvaluateRule(1,rules);

<<<<<<< HEAD
//            Measure measure = new Measure("accu1","accu1 description", Measure.MearuseType.accuracy, "bullyeye", source, target, eRule,"test1");
//            measureRepo.save(measure);
=======
        Measure measure = new Measure("bevssoj","bevssoj description", Measure.MearuseType.accuracy, "bullyeye", source, target, eRule,"test1");
        measureRepo.save(measure);

        DataConnector source2 = new DataConnector(ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target2 = new DataConnector(ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule2 = new EvaluateRule(1,rules);
        Measure measure2 = new Measure("test","test description", Measure.MearuseType.accuracy, "bullyeye", source2, target2, eRule2,"test1");
        measureRepo.save(measure2);

        DataConnector source3 = new DataConnector(ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target3 = new DataConnector(ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule3 = new EvaluateRule(1,rules);
        Measure measure3 = new Measure("just_inthere","test_just_inthere description", Measure.MearuseType.accuracy, "hadoop", source3, target3, eRule3,"test1");
        measureRepo.save(measure3);
>>>>>>> b5fce52475bf275d6ff3fd97a1e0eee7b4a4d3fc
    }

    @Bean
    public SimpleCORSFilter simpleFilter() {
        return new SimpleCORSFilter();
    }

}
