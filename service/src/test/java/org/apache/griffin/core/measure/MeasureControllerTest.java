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

import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.EvaluateRule;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest(value=MeasureController.class,secure = false)
public class MeasureControllerTest {
    @Autowired
    private MockMvc mvc;

    @MockBean
    private MeasureService service;

    @Before
    public void setup(){
    }

    @Test
    public void testGetAllMeasures() throws IOException,Exception{
        Measure measure = createATestMeasure("viewitem_hourly","bullseye");

        given(service.getAllMeasures()).willReturn(Arrays.asList(measure));

        mvc.perform(get("/measures").contentType(MediaType.APPLICATION_JSON))
//                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].name",is("viewitem_hourly")))
        ;
    }


    @Test
    public void testGetMeasuresById() throws IOException,Exception{
        Measure measure = createATestMeasure("viewitem_hourly","bullseye");

        given(service.getMeasuresById(1L)).willReturn(measure);

        mvc.perform(get("/measures/1").contentType(MediaType.APPLICATION_JSON))
//                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name",is("viewitem_hourly")))
        ;
    }

    @Test
    public void testGetMeasureByName() throws IOException,Exception{
        Measure measure = createATestMeasure("viewitem_hourly","bullseye");

        given(service.getMeasuresByName("viewitem_hourly")).willReturn(measure);

        mvc.perform(get("/measures/findByName/viewitem_hourly").contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name",is("viewitem_hourly")))
        ;
    }
    @Test
    public void testDeleteMeasuresById() throws Exception{
        Mockito.doNothing().when(service).deleteMeasuresById(1L);

        mvc.perform(delete("/measures/deleteById/1").contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
        ;
    }

    @Test
    public void testDeleteMeasuresByName() throws Exception{
        String measureName="viewitem_hourly";
        given(service.deleteMeasuresByName(measureName)).willReturn(GriffinOperationMessage.DELETE_MEASURE_BY_NAME_SUCCESS);

        mvc.perform(delete("/measures/deleteByName/"+measureName).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$",is("DELETE_MEASURE_BY_NAME_SUCCESS")))
        ;
    }

    @Test
    public void testUpdateMeasure() throws Exception{
        String measureName="viewitem_hourly";
        String org="bullseye";
        Measure measure=createATestMeasure(measureName,org);
        ObjectMapper mapper=new ObjectMapper();
        String measureJson=mapper.writeValueAsString(measure);
        given(service.updateMeasure(measure)).willReturn(GriffinOperationMessage.UPDATE_MEASURE_SUCCESS);

        mvc.perform(post("/measures/update").contentType(MediaType.APPLICATION_JSON).content(measureJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$",is("UPDATE_MEASURE_SUCCESS")))
        ;
    }

    @Test
    public void testGetAllMeasureNameOfOwner() throws Exception{
        String Owner="test1";
        String measureName="viewitem_hourly";
        given(service.getAllMeasureNameByOwner(Owner)).willReturn(Arrays.asList(measureName));

        mvc.perform(get("/measures/owner/"+Owner).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0]",is("viewitem_hourly")))
        ;
    }

    @Test
    public void testCreateNewMeasure() throws Exception{
        String measureName="viewitem_hourly";
        String org="bullseye";
        Measure measure=createATestMeasure(measureName,org);
        ObjectMapper mapper=new ObjectMapper();
        String measureJson=mapper.writeValueAsString(measure);
        given(service.createNewMeasure(measure)).willReturn(GriffinOperationMessage.CREATE_MEASURE_SUCCESS);

        mvc.perform(post("/measures/add").contentType(MediaType.APPLICATION_JSON).content(measureJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$",is("CREATE_MEASURE_SUCCESS")))
        ;
    }

    private Measure createATestMeasure(String name,String org)throws IOException,Exception{
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

        Measure measure = new Measure(name,"bevssoj description", Measure.MearuseType.accuracy, org, source, target, eRule,"test1");

        return measure;
    }


}
