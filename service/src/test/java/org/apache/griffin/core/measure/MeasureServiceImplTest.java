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


import org.apache.griffin.core.job.JobServiceImpl;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.DataConnectorRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.griffin.core.util.EntityHelper.createATestGriffinMeasure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

@RunWith(SpringRunner.class)
public class MeasureServiceImplTest {


    @InjectMocks
    private MeasureServiceImpl service;
    @Mock
    private MeasureRepo measureRepo;
    @Mock
    private JobServiceImpl jobService;

    @Mock
    private DataConnectorRepo dataConnectorRepo;

    @Before
    public void setup() {
    }

    @Test
    public void testGetAllMeasures() throws Exception {
        Measure measure = createATestGriffinMeasure("view_item_hourly", "test");
        given(measureRepo.findByDeleted(false)).willReturn(Arrays.asList(measure));
        List<Measure> measures = (List<Measure>) service.getAllAliveMeasures();
        assertThat(measures.size()).isEqualTo(1);
        assertThat(measures.get(0).getName()).isEqualTo("view_item_hourly");
    }

    @Test
    public void testGetMeasuresById() throws Exception {
        Measure measure = createATestGriffinMeasure("view_item_hourly", "test");
        given(measureRepo.findByIdAndDeleted(1L, false)).willReturn(measure);
        Measure m = service.getMeasureById(1);
        assertEquals(m.getName(), measure.getName());
    }


//    @Test
//    public void testDeleteMeasuresByIdForSuccess() throws Exception {
//        GriffinMeasure measure = createATestGriffinMeasure("view_item_hourly", "test");
//        given(measureRepo.findByIdAndDeleted(measure.getId(),false)).willReturn(measure);
//        given(jobService.deleteJobsRelateToMeasure(measure.getId())).willReturn(true);
//        GriffinOperationMessage message = service.deleteMeasureById(measure.getId());
//        assertEquals(message, GriffinOperationMessage.DELETE_MEASURE_BY_ID_SUCCESS);
//    }

    @Test
    public void testDeleteMeasuresByIdForNotFound() throws Exception {
        given(measureRepo.exists(1L)).willReturn(false);
        GriffinOperationMessage message = service.deleteMeasureById(1L);
        assertEquals(message, GriffinOperationMessage.RESOURCE_NOT_FOUND);
    }

//    @Test
//    public void testCreateNewMeasureForSuccess() throws Exception {
//        String measureName = "view_item_hourly";
//        Measure measure = createATestGriffinMeasure(measureName, "test");
//        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new LinkedList<>());
//        given(measureRepo.save(measure)).willReturn(measure);
//        GriffinOperationMessage message = service.createMeasure(measure);
//        assertEquals(message, GriffinOperationMessage.CREATE_MEASURE_SUCCESS);
//    }

//    @Test
//    public void testCreateNewMeasureForFailureWithConnectorNameRepeat() throws Exception {
//        String measureName = "view_item_hourly";
//        Measure measure = createATestGriffinMeasure(measureName, "test");
//        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new LinkedList<>());
//        DataConnector dc = new DataConnector("name", "", "", "");
//        given(dataConnectorRepo.findByConnectorNames(Matchers.any())).willReturn(Arrays.asList(dc));
//        given(measureRepo.save(measure)).willReturn(measure);
//        GriffinOperationMessage message = service.createMeasure(measure);
//        assertEquals(message, GriffinOperationMessage.CREATE_MEASURE_FAIL);
//    }

    @Test
    public void testCreateNewMeasureForFailWithMeasureDuplicate() throws Exception {
        String measureName = "view_item_hourly";
        Measure measure = createATestGriffinMeasure(measureName, "test");
        LinkedList<Measure> list = new LinkedList<>();
        list.add(measure);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(list);
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE);
    }

//    @Test
//    public void testCreateNewMeasureForFailWithSaveException() throws Exception {
//        String measureName = "view_item_hourly";
//        Measure measure = createATestGriffinMeasure(measureName, "test");
//        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new LinkedList<>());
//        given(measureRepo.save(measure)).willReturn(null);
//        GriffinOperationMessage message = service.createMeasure(measure);
//        assertEquals(message, GriffinOperationMessage.CREATE_MEASURE_FAIL);
//    }

    @Test
    public void testGetAllMeasureByOwner() throws Exception {
        String owner = "test";
        Measure measure = createATestGriffinMeasure("view_item_hourly", "test");
        measure.setId(1L);
        given(measureRepo.findByOwnerAndDeleted(owner, false)).willReturn(Arrays.asList(measure));
        List<Measure> list = service.getAliveMeasuresByOwner(owner);
        assertEquals(list.get(0).getName(), measure.getName());
    }

//    @Test
//    public void testUpdateMeasureForSuccess() throws Exception {
//        Measure measure = createATestGriffinMeasure("view_item_hourly", "test");
//        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(new GriffinMeasure());
//        given(measureRepo.save(measure)).willReturn(measure);
//        GriffinOperationMessage message = service.updateMeasure(measure);
//        assertEquals(message, GriffinOperationMessage.UPDATE_MEASURE_SUCCESS);
//    }

    @Test
    public void testUpdateMeasureForNotFound() throws Exception {
        Measure measure = createATestGriffinMeasure("view_item_hourly", "test");
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(null);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, GriffinOperationMessage.RESOURCE_NOT_FOUND);
    }

//    @Test
//    public void testUpdateMeasureForFailWithSaveException() throws Exception {
//        Measure measure = createATestGriffinMeasure("view_item_hourly", "test");
//        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(new GriffinMeasure());
//        given(measureRepo.save(measure)).willThrow(Exception.class);
//        GriffinOperationMessage message = service.updateMeasure(measure);
//        assertEquals(message, GriffinOperationMessage.UPDATE_MEASURE_FAIL);
//    }

}
