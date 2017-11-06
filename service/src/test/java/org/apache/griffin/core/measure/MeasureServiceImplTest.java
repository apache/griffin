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
import java.util.Map;

import static org.apache.griffin.core.measure.MeasureTestHelper.createATestMeasure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;

@RunWith(SpringRunner.class)
public class MeasureServiceImplTest {


    @InjectMocks
    private MeasureServiceImpl service;
    @Mock
    private MeasureRepo measureRepo;
    @Mock
    private JobServiceImpl jobService;

    @Before
    public void setup() {
    }

    @Test
    public void testGetAllMeasures() throws Exception {
        Measure measure = createATestMeasure("view_item_hourly", "test");
        given(measureRepo.findByDeleted(false)).willReturn(Arrays.asList(measure));
        List<Measure> measures = (List<Measure>) service.getAllAliveMeasures();
        assertThat(measures.size()).isEqualTo(1);
        assertThat(measures.get(0).getName()).isEqualTo("view_item_hourly");
    }

    @Test
    public void testGetMeasuresById() throws Exception {
        Measure measure = createATestMeasure("view_item_hourly", "test");
        given(measureRepo.findByIdAndDeleted(1L,false)).willReturn(measure);
        Measure m = service.getMeasureById(1);
        assertEquals(m.getName(), measure.getName());
    }


    @Test
    public void testDeleteMeasuresByIdForSuccess() throws Exception {
        Measure measure = createATestMeasure("view_item_hourly", "test");
        given(measureRepo.exists(1L)).willReturn(true);
        given(measureRepo.findOne(1L)).willReturn(measure);
        doNothing().when(jobService).deleteJobsRelateToMeasure(measure);
        given(measureRepo.save(measure)).willReturn(measure);
        GriffinOperationMessage message = service.deleteMeasureById(1L);
        assertEquals(message, GriffinOperationMessage.DELETE_MEASURE_BY_ID_SUCCESS);
    }

    @Test
    public void testDeleteMeasuresByIdForNotFound() throws Exception {
        given(measureRepo.exists(1L)).willReturn(false);
        GriffinOperationMessage message = service.deleteMeasureById(1L);
        assertEquals(message, GriffinOperationMessage.RESOURCE_NOT_FOUND);
    }

    @Test
    public void testCreateNewMeasureForSuccess() throws Exception {
        String measureName = "view_item_hourly";
        Measure measure = createATestMeasure(measureName, "test");
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new LinkedList<>());
        given(measureRepo.save(measure)).willReturn(measure);
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, GriffinOperationMessage.CREATE_MEASURE_SUCCESS);
    }

    @Test
    public void testCreateNewMeasureForFailWithDuplicate() throws Exception {
        String measureName = "view_item_hourly";
        Measure measure = createATestMeasure(measureName, "test");
        LinkedList<Measure> list = new LinkedList<>();
        list.add(measure);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(list);
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE);
    }

    @Test
    public void testCreateNewMeasureForFailWithSaveException() throws Exception {
        String measureName = "view_item_hourly";
        Measure measure = createATestMeasure(measureName, "test");
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new LinkedList<>());
        given(measureRepo.save(measure)).willReturn(null);
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, GriffinOperationMessage.CREATE_MEASURE_FAIL);
    }

    @Test
    public void testGetAllMeasureByOwner() throws Exception {
        String owner = "test";
        Measure measure = createATestMeasure("view_item_hourly", "test");
        measure.setId(1L);
        given(measureRepo.findByOwnerAndDeleted(owner, false)).willReturn(Arrays.asList(measure));
        List<Measure> list = service.getAliveMeasuresByOwner(owner);
        assertEquals(list.get(0).getName(), measure.getName());
    }

    @Test
    public void testUpdateMeasureForSuccess() throws Exception {
        Measure measure = createATestMeasure("view_item_hourly", "test");
        given(measureRepo.findByIdAndDeleted(measure.getId(),false)).willReturn(new Measure());
        given(measureRepo.save(measure)).willReturn(measure);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, GriffinOperationMessage.UPDATE_MEASURE_SUCCESS);
    }

    @Test
    public void testUpdateMeasureForNotFound() throws Exception {
        Measure measure = createATestMeasure("view_item_hourly", "test");
        given(measureRepo.findByIdAndDeleted(measure.getId(),false)).willReturn(null);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, GriffinOperationMessage.RESOURCE_NOT_FOUND);
    }

    @Test
    public void testUpdateMeasureForFailWithSaveException() throws Exception {
        Measure measure = createATestMeasure("view_item_hourly", "test");
        given(measureRepo.findByIdAndDeleted(measure.getId(),false)).willReturn(new Measure());
        given(measureRepo.save(measure)).willThrow(Exception.class);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, GriffinOperationMessage.UPDATE_MEASURE_FAIL);
    }

}
