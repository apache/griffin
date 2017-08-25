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

package org.apache.griffin.core.measure;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.griffin.core.job.JobServiceImpl;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.EvaluateRule;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;

@RunWith(SpringRunner.class)
public class MeasureServiceImplTest {

    @TestConfiguration
    public static class MeasureServiceImplConfiguration{
        @Bean
        public MeasureServiceImpl service(){
            return new MeasureServiceImpl();
        }

        @Bean
        public JobServiceImpl JobService(){
            return new JobServiceImpl();
        }

    }
    @MockBean
    private MeasureRepo measureRepo;

    @Autowired
    private MeasureServiceImpl service;


    @Before
    public void setup(){
    }

    @Test
    public void testGetAllMeasures(){
        try {
            Iterable<Measure> tmp = service.getAllAliveMeasures();
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all Measure from dbs");
        }
    }

    @Test
    public void testGetMeasuresById(){
        try {
            Measure tmp = service.getMeasureById(1);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get Measure in db By Id: 1");
        }
    }

  /*  @Test
    public void testGetMeasuresByName(){
        try {
            Measure tmp = service.getMeasureByName("viewitem_hourly");
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get Measure in db By name: viewitem_hourly");
        }
    }*/

    @Test
    public void testDeleteMeasuresById(){
        try {
            service.deleteMeasureById(1L);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot delete Measure in db By Id: 1");
        }
    }

    /*@Test
    public void testDeleteMeasuresByName(){
        try {
            String measureName="viewitem_hourly";
            given(measureRepo.findByName(measureName)).willReturn(null);
            GriffinOperationMessage message=service.deleteMeasureByName("viewitem_hourly");
            assertEquals(message,GriffinOperationMessage.RESOURCE_NOT_FOUND);
            assertTrue(true);

            String org="bullseye";
            Measure measure=createATestMeasure(measureName,org);
            given(measureRepo.findByName(measureName)).willReturn(measure);
            GriffinOperationMessage message1=service.deleteMeasureByName("viewitem_hourly");
            assertEquals(message1,GriffinOperationMessage.DELETE_MEASURE_BY_NAME_SUCCESS);
        }catch (Throwable t){
            fail("Cannot delete Measure in db By name: viewitem_hourly");
        }
    }*/

    @Test
    public void testCreateNewMeasure(){
        try {
            String measureName="viewitem_hourly";
            String org="bullseye";
            Measure measure=createATestMeasure(measureName,org);
            given(measureRepo.findOne(0L)).willReturn(null);
            GriffinOperationMessage message=service.createMeasure(measure);
            assertEquals(message,GriffinOperationMessage.CREATE_MEASURE_FAIL);
            assertTrue(true);

            Measure measure1=createATestMeasure(measureName,"bullseye1");
            given(measureRepo.findOne(0L)).willReturn(measure1);
            GriffinOperationMessage message1=service.createMeasure(measure);
            assertEquals(message1,GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE);

            given(measureRepo.findOne(0L)).willReturn(null);
            given(measureRepo.save(measure)).willReturn(measure);
            GriffinOperationMessage message2=service.createMeasure(measure);
            assertEquals(message2,GriffinOperationMessage.CREATE_MEASURE_SUCCESS);
        }catch (Throwable t){
            fail("Cannot create new measure viewitem_hourly");
        }
    }

    @Test
    public void testGetAllMeasureByOwner(){
        try {
            String measureName="viewitem_hourly";
            String org="bullseye";
            Measure measure=createATestMeasure(measureName,org);
            String owner="test1";
            given(measureRepo.findAll()).willReturn(Arrays.asList(measure));
            List<Map<String, String>> namelist=service.getAllAliveMeasureNameIdByOwner(owner);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all measure name by owner test1");
        }
    }

    @Test
    public void testUpdateMeasure(){
        try {
            String measureName="viewitem_hourly";
            String org="bullseye";
            Measure measure=createATestMeasure(measureName,org);
            GriffinOperationMessage message=service.updateMeasure(measure);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot create new measure viewitem_hourly");
        }
    }

    private Measure createATestMeasure(String name,String org)throws IOException,Exception{
        HashMap<String,String> configMap1;
        configMap1 = new HashMap<>();
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
        Measure measure = new Measure(name,"bevssoj description", Measure.MearuseType.accuracy, org, source, target, eRule,"test1");
        measure.setId(0L);
        return measure;
    }

}
