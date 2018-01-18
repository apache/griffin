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

import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.griffin.core.util.URLHelper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.griffin.core.util.EntityHelper.createGriffinMeasure;
import static org.apache.griffin.core.util.GriffinOperationMessage.*;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest(value = MeasureController.class, secure = false)
public class MeasureControllerTest {
    @Autowired
    private MockMvc mvc;

    @MockBean
    private MeasureService service;


    @Before
    public void setup() {

    }

    @Test
    public void testGetAllMeasures() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        given(service.getAllAliveMeasures()).willReturn(Arrays.asList(measure));

        mvc.perform(get(URLHelper.API_VERSION_PATH + "/measures"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].name", is("view_item_hourly")));
    }


    @Test
    public void testGetMeasuresById() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        given(service.getMeasureById(1L)).willReturn(measure);

        mvc.perform(get(URLHelper.API_VERSION_PATH + "/measures/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is("view_item_hourly")))
        ;
    }

    @Test
    public void testDeleteMeasuresByIdForSuccess() throws Exception {
        given(service.deleteMeasureById(1L)).willReturn(DELETE_MEASURE_BY_ID_SUCCESS);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/measures/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(202)));
    }

    @Test
    public void testDeleteMeasuresByIdForNotFound() throws Exception {
        given(service.deleteMeasureById(1L)).willReturn(RESOURCE_NOT_FOUND);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/measures/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(400)));
    }

    @Test
    public void testDeleteMeasuresByIdForFail() throws Exception {
        given(service.deleteMeasureById(1L)).willReturn(DELETE_MEASURE_BY_ID_FAIL);

        mvc.perform(delete(URLHelper.API_VERSION_PATH + "/measures/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(402)));
    }

    @Test
    public void testUpdateMeasureForSuccess() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        String measureJson = JsonUtil.toJson(measure);
        given(service.updateMeasure(measure)).willReturn(UPDATE_MEASURE_SUCCESS);

        mvc.perform(put(URLHelper.API_VERSION_PATH + "/measures")
                .contentType(MediaType.APPLICATION_JSON).content(measureJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(204)));
    }

    @Test
    public void testUpdateMeasureForNotFound() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        String measureJson = JsonUtil.toJson(measure);
        given(service.updateMeasure(measure)).willReturn(RESOURCE_NOT_FOUND);

        mvc.perform(put(URLHelper.API_VERSION_PATH + "/measures")
                .contentType(MediaType.APPLICATION_JSON).content(measureJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(400)));

    }

    @Test
    public void testUpdateMeasureForFail() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        String measureJson = JsonUtil.toJson(measure);
        given(service.updateMeasure(measure)).willReturn(UPDATE_MEASURE_FAIL);

        mvc.perform(put(URLHelper.API_VERSION_PATH + "/measures")
                .contentType(MediaType.APPLICATION_JSON).content(measureJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(404)));
    }

    @Test
    public void testGetAllMeasuresByOwner() throws Exception {
        String owner = "test";
        List<Measure> measureList = new LinkedList<>();
        Measure measure = createGriffinMeasure("view_item_hourly");
        measureList.add(measure);
        given(service.getAliveMeasuresByOwner(owner)).willReturn(measureList);

        mvc.perform(get(URLHelper.API_VERSION_PATH + "/measures/owner/" + owner)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].name", is("view_item_hourly")))
        ;
    }

    @Test
    public void testCreateNewMeasureForSuccess() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        String measureJson = JsonUtil.toJson(measure);
        given(service.createMeasure(measure)).willReturn(CREATE_MEASURE_SUCCESS);

        mvc.perform(post(URLHelper.API_VERSION_PATH + "/measures")
                .contentType(MediaType.APPLICATION_JSON).content(measureJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(201)));
    }

    @Test
    public void testCreateNewMeasureForFailWithDuplicate() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        String measureJson = JsonUtil.toJson(measure);
        given(service.createMeasure(measure)).willReturn(CREATE_MEASURE_FAIL_DUPLICATE);

        mvc.perform(post(URLHelper.API_VERSION_PATH + "/measures")
                .contentType(MediaType.APPLICATION_JSON).content(measureJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(410)));
    }

    @Test
    public void testCreateNewMeasureForFailWithSaveException() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        String measureJson = JsonUtil.toJson(measure);
        given(service.createMeasure(measure)).willReturn(GriffinOperationMessage.CREATE_MEASURE_FAIL);

        mvc.perform(post(URLHelper.API_VERSION_PATH + "/measures").contentType(MediaType.APPLICATION_JSON).content(measureJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(401)));
    }


}
