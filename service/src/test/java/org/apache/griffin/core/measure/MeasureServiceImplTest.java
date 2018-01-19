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
import org.apache.griffin.core.job.repo.VirtualJobRepo;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.ExternalMeasure;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.DataConnectorRepo;
import org.apache.griffin.core.measure.repo.ExternalMeasureRepo;
import org.apache.griffin.core.measure.repo.GriffinMeasureRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.griffin.core.util.EntityHelper.*;
import static org.apache.griffin.core.util.GriffinOperationMessage.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

@RunWith(SpringRunner.class)
public class MeasureServiceImplTest {

    @TestConfiguration
    public static class MeasureServiceConf {
        @Bean
        public MeasureServiceImpl measureService() {
            return new MeasureServiceImpl();
        }

        @Bean(name = "griffinOperation")
        public MeasureOperation griffinOperation() {
            return new GriffinMeasureOperationImpl();
        }

        @Bean(name = "externalOperation")
        public MeasureOperation externalOperation() {
            return new ExternalMeasureOperationImpl();
        }
    }

    @Autowired
    private MeasureServiceImpl service;

    @MockBean
    private ExternalMeasureRepo externalMeasureRepo;

    @MockBean
    private GriffinMeasureRepo griffinMeasureRepo;

    @MockBean
    private MeasureRepo<Measure> measureRepo;

    @MockBean
    private JobServiceImpl jobService;

    @MockBean
    private DataConnectorRepo dataConnectorRepo;

    @MockBean
    private VirtualJobRepo jobRepo;

    @Before
    public void setup() {
    }

    @Test
    public void testGetAllMeasures() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        given(measureRepo.findByDeleted(false)).willReturn(Arrays.asList(measure));
        List<Measure> measures = service.getAllAliveMeasures();
        assertEquals(measures.size(), 1);
        assertEquals(measures.get(0).getName(), "view_item_hourly");
    }

    @Test
    public void testGetMeasuresById() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        given(measureRepo.findByIdAndDeleted(1L, false)).willReturn(measure);
        Measure m = service.getMeasureById(1);
        assertEquals(m.getName(), measure.getName());
    }

    @Test
    public void testGetAliveMeasuresByOwner() throws Exception {
        String owner = "test";
        Measure measure = createGriffinMeasure("view_item_hourly");
        given(measureRepo.findByOwnerAndDeleted(owner, false)).willReturn(Arrays.asList(measure));
        List<Measure> measures = service.getAliveMeasuresByOwner(owner);
        assertEquals(measures.get(0).getName(), measure.getName());
    }


    @Test
    public void testDeleteMeasuresByIdForGriffinSuccess() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("view_item_hourly");
        measure.setId(1L);
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        given(jobService.deleteJobsRelateToMeasure(measure.getId())).willReturn(true);
        GriffinOperationMessage message = service.deleteMeasureById(measure.getId());
        assertEquals(message, DELETE_MEASURE_BY_ID_SUCCESS);
    }

    @Test
    public void testDeleteMeasuresByIdForGriffinFailureWithPause() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("view_item_hourly");
        measure.setId(1L);
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        given(jobService.deleteJobsRelateToMeasure(measure.getId())).willReturn(false);
        GriffinOperationMessage message = service.deleteMeasureById(measure.getId());
        assertEquals(message, DELETE_MEASURE_BY_ID_FAIL);
    }

    @Test
    public void testDeleteMeasuresByIdForGriffinFailureWithException() throws Exception {
        GriffinMeasure measure = createGriffinMeasure("view_item_hourly");
        measure.setId(1L);
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        given(jobService.deleteJobsRelateToMeasure(measure.getId())).willReturn(true);
        given(measureRepo.save(Matchers.any(Measure.class))).willThrow(Exception.class);
        GriffinOperationMessage message = service.deleteMeasureById(measure.getId());
        assertEquals(message, DELETE_MEASURE_BY_ID_FAIL);
    }

    @Test
    public void testDeleteMeasuresByIdForExternalSuccess() throws Exception {
        ExternalMeasure measure = createExternalMeasure("externalMeasure");
        measure.setId(1L);
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        GriffinOperationMessage message = service.deleteMeasureById(measure.getId());
        assertEquals(message, DELETE_MEASURE_BY_ID_SUCCESS);
    }

    @Test
    public void testDeleteMeasuresByIdForExternalFailureWithException() throws Exception {
        ExternalMeasure measure = createExternalMeasure("externalMeasure");
        measure.setId(1L);
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        given(externalMeasureRepo.save(Matchers.any(ExternalMeasure.class))).willThrow(Exception.class);
        GriffinOperationMessage message = service.deleteMeasureById(measure.getId());
        assertEquals(message, DELETE_MEASURE_BY_ID_FAIL);
    }

    @Test
    public void testDeleteMeasuresByIdForFailureWithNotFound() throws Exception {
        given(measureRepo.findByIdAndDeleted(1L, false)).willReturn(null);
        GriffinOperationMessage message = service.deleteMeasureById(1L);
        assertEquals(message, RESOURCE_NOT_FOUND);
    }

    @Test
    public void testCreateMeasureForGriffinSuccess() throws Exception {
        String measureName = "view_item_hourly";
        GriffinMeasure measure = createGriffinMeasure(measureName);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new ArrayList<>());
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, CREATE_MEASURE_SUCCESS);
    }

    @Test
    public void testCreateMeasureForGriffinFailureWithConnectorExist() throws Exception {
        String measureName = "view_item_hourly";
        GriffinMeasure measure = createGriffinMeasure(measureName);
        DataConnector dc = new DataConnector("source_name", "1h", "1.2", null);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new LinkedList<>());
        given(dataConnectorRepo.findByConnectorNames(Arrays.asList("source_name", "target_name"))).willReturn(Arrays.asList(dc));
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, CREATE_MEASURE_FAIL);
    }

    @Test
    public void testCreateMeasureForGriffinFailureWithConnectorNull() throws Exception {
        String measureName = "view_item_hourly";
        DataConnector dcSource = createDataConnector(null, "default", "test_data_src", "dt=#YYYYMMdd# AND hour=#HH#");
        DataConnector dcTarget = createDataConnector(null, "default", "test_data_tgt", "dt=#YYYYMMdd# AND hour=#HH#");
        GriffinMeasure measure = createGriffinMeasure(measureName, dcSource, dcTarget);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new LinkedList<>());
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, CREATE_MEASURE_FAIL);
    }

    @Test
    public void testCreateMeasureForGriffinFailureWithException() throws Exception {
        String measureName = "view_item_hourly";
        GriffinMeasure measure = createGriffinMeasure(measureName);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new ArrayList<>());
        given(measureRepo.save(Matchers.any(Measure.class))).willThrow(Exception.class);
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, CREATE_MEASURE_FAIL);
    }

    @Test
    public void testCreateMeasureForExternalSuccess() throws Exception {
        String measureName = "view_item_hourly";
        ExternalMeasure measure = createExternalMeasure(measureName);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new ArrayList<>());
        given(externalMeasureRepo.save(measure)).willReturn(measure);
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, CREATE_MEASURE_SUCCESS);
    }

    @Test
    public void testCreateMeasureForExternalFailureWithBlank() throws Exception {
        String measureName = "view_item_hourly";
        ExternalMeasure measure = createExternalMeasure(measureName);
        measure.setMetricName("  ");
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new ArrayList<>());
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, CREATE_MEASURE_FAIL);
    }

    @Test
    public void testCreateMeasureForExternalFailureWithException() throws Exception {
        String measureName = "view_item_hourly";
        ExternalMeasure measure = createExternalMeasure(measureName);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(new ArrayList<>());
        given(externalMeasureRepo.save(measure)).willReturn(measure);
        given(externalMeasureRepo.save(Matchers.any(ExternalMeasure.class))).willThrow(Exception.class);
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, CREATE_MEASURE_FAIL);
    }

    @Test
    public void testCreateMeasureForFailureWithRepeat() throws Exception {
        String measureName = "view_item_hourly";
        GriffinMeasure measure = createGriffinMeasure(measureName);
        given(measureRepo.findByNameAndDeleted(measureName, false)).willReturn(Arrays.asList(measure));
        GriffinOperationMessage message = service.createMeasure(measure);
        assertEquals(message, CREATE_MEASURE_FAIL_DUPLICATE);
    }


    @Test
    public void testUpdateMeasureForGriffinSuccess() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, UPDATE_MEASURE_SUCCESS);
    }

    @Test
    public void testUpdateMeasureForGriffinFailureWithDiffType() throws Exception {
        Measure griffinMeasure = createGriffinMeasure("view_item_hourly");
        Measure externalMeasure = createExternalMeasure("externalName");
        given(measureRepo.findByIdAndDeleted(griffinMeasure.getId(), false)).willReturn(externalMeasure);
        GriffinOperationMessage message = service.updateMeasure(griffinMeasure);
        assertEquals(message, UPDATE_MEASURE_FAIL);
    }

    @Test
    public void testUpdateMeasureForGriffinFailureWithNotFound() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(null);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, RESOURCE_NOT_FOUND);
    }

    @Test
    public void testUpdateMeasureForGriffinFailureWithException() throws Exception {
        Measure measure = createGriffinMeasure("view_item_hourly");
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        given(measureRepo.save(Matchers.any(Measure.class))).willThrow(Exception.class);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, UPDATE_MEASURE_FAIL);
    }

    @Test
    public void testUpdateMeasureForExternalSuccess() throws Exception {
        ExternalMeasure measure = createExternalMeasure("external_view_item_hourly");
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        given(externalMeasureRepo.findOne(measure.getId())).willReturn(measure);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, UPDATE_MEASURE_SUCCESS);
    }

    @Test
    public void testUpdateMeasureForExternalFailureWithBlank() throws Exception {
        String measureName = "view_item_hourly";
        ExternalMeasure measure = createExternalMeasure(measureName);
        measure.setMetricName("  ");
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, UPDATE_MEASURE_FAIL);
    }

    @Test
    public void testUpdateMeasureForExternalFailWithException() throws Exception {
        ExternalMeasure measure = createExternalMeasure("external_view_item_hourly");
        given(measureRepo.findByIdAndDeleted(measure.getId(), false)).willReturn(measure);
        given(externalMeasureRepo.findOne(measure.getId())).willReturn(measure);
        given(externalMeasureRepo.save(Matchers.any(ExternalMeasure.class))).willThrow(Exception.class);
        GriffinOperationMessage message = service.updateMeasure(measure);
        assertEquals(message, GriffinOperationMessage.UPDATE_MEASURE_FAIL);
    }

}
