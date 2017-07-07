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
package org.apache.griffin.core;


import org.apache.griffin.core.common.SimpleCORSFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

//import org.apache.griffin.core.measure.repo.ConnectorConfigRepo;

@SpringBootApplication
public class GriffinWebApplication/* implements CommandLineRunner*/{
    private static final Logger log = LoggerFactory.getLogger(GriffinWebApplication.class);
    public static void main(String[] args) {
        log.info("application start");
        SpringApplication.run(GriffinWebApplication.class, args);
    }

//    @Autowired
//    MeasureRepo measureRepo;
//    @Autowired
//    EvaluateRuleRepo evaluateRuleRepo;
//    @Autowired
//    DataConnectorRepo connectorRepo;
//
//    public void run(String... strings) throws Exception {
//        HashMap<String,String> configMap1=new HashMap<>();
//        configMap1.put("database","default");
//        configMap1.put("table.name","test_data_src");
//        HashMap<String,String> configMap2=new HashMap<>();
//        configMap2.put("database","default");
//        configMap2.put("table.name","test_data_tgt");
//        String configJson1 = new ObjectMapper().writeValueAsString(configMap1);
//        String configJson2 = new ObjectMapper().writeValueAsString(configMap2);
//
//        DataConnector source = new DataConnector(ConnectorType.HIVE, "1.2", configJson1);
//        DataConnector target = new DataConnector(ConnectorType.HIVE, "1.2", configJson2);
//
//        String rules = "$source.uage > 100 AND $source.uid = $target.uid AND $source.uage + 12 = $target.uage + 10 + 2 AND $source.udes + 11 = $target.udes + 1 + 1";
//
//        EvaluateRule eRule = new EvaluateRule(1,rules);
//
//        Measure measure = new Measure("viewitem_hourly","bevssoj description", Measure.MearuseType.accuracy, "bullseye", source, target, eRule,"test1");
//        measureRepo.save(measure);
//
//        DataConnector source2 = new DataConnector(ConnectorType.HIVE, "1.2", configJson1);
//        DataConnector target2 = new DataConnector(ConnectorType.HIVE, "1.2", configJson2);
//        EvaluateRule eRule2 = new EvaluateRule(1,rules);
//        Measure measure2 = new Measure("search_hourly","test description", Measure.MearuseType.accuracy, "bullseye", source2, target2, eRule2,"test1");
//        measureRepo.save(measure2);
//
//        DataConnector source3 = new DataConnector(ConnectorType.HIVE, "1.2", configJson1);
//        DataConnector target3 = new DataConnector(ConnectorType.HIVE, "1.2", configJson2);
//        EvaluateRule eRule3 = new EvaluateRule(1,rules);
//        Measure measure3 = new Measure("buy_hourly","test_just_inthere description", Measure.MearuseType.accuracy, "bullseye", source3, target3, eRule3,"test1");
//        measureRepo.save(measure3);
//    }

    @Bean
    public SimpleCORSFilter simpleFilter() {
        return new SimpleCORSFilter();
    }

}