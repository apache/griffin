/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

package org.apache.griffin.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.griffin.core.measure.DataConnector;
import org.apache.griffin.core.measure.EvaluateRule;
import org.apache.griffin.core.measure.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Created by xiangrchen on 5/16/17.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
public class GriffinControllerTest {
    private MockMvc mockMvc;

    @Mock
    MeasureRepo measureRepo;

    @InjectMocks
    private GriffinController griffinController;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.standaloneSetup(griffinController).build();
    }

    @Test
    public void test_greeting() throws Exception {
        mockMvc.perform(get("/version"))
                .andExpect(status().isOk())
                .andExpect(content().string(is("0.1.0")));
    }

    @Test
    public void test_getOrgs() throws Exception {
        when(measureRepo.findOrganizations()).thenReturn(new ArrayList<String>());
        mockMvc.perform(get("/org"))
                .andExpect(status().isOk());
        verify(measureRepo).findOrganizations();
    }

    @Test
    public void test_getMetricNameListByOrg() throws Exception{
        String org="hadoop";
        when(measureRepo.findNameByOrganization(org)).thenReturn(new ArrayList<String>());
        mockMvc.perform(get("/org/{org}",org))
                .andExpect(status().isOk())
                .andExpect(content().string(is("[]")));
        verify(measureRepo).findNameByOrganization(org);
    }

    @Test
    public void test_getOrgsWithMetrics() throws Exception{
        String org="hadoop";
        List<String> orgList=new ArrayList<>(Arrays.asList(org));
        when(measureRepo.findOrganizations()).thenReturn(orgList);

        when(measureRepo.findNameByOrganization(org)).thenReturn(Arrays.asList("viewitem_hourly"));
        mockMvc.perform(get("/orgWithMetrics"))
                .andExpect(status().isOk());
        verify(measureRepo).findOrganizations();
        verify(measureRepo).findNameByOrganization(org);
    }

    @Test
    public void test_getMeasureNameByDataAssets() throws Exception{
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
        Measure measure = new Measure("viewitem_hourly","bevssoj description", Measure.MearuseType.accuracy, "bullyeye", source, target, eRule,"test1");

        DataConnector source2 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target2 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule2 = new EvaluateRule(1,rules);
        Measure measure2 = new Measure("search_hourly","test description", Measure.MearuseType.accuracy, "bullyeye", source2, target2, eRule2,"test1");

        when(measureRepo.findAll()).thenReturn(Arrays.asList(measure,measure2));
        mockMvc.perform(get("/dataAssetsWithMetrics"))
                .andExpect(status().isOk())
                .andDo(print());
        verify(measureRepo).findAll();
    }
}
