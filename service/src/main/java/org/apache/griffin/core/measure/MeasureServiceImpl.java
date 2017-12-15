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


import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.job.JobSyncHelper;
import org.apache.griffin.core.measure.entity.*;
import org.apache.griffin.core.measure.repo.DataConnectorRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Service
public class MeasureServiceImpl implements MeasureService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MeasureServiceImpl.class);

    @Autowired
    private JobSyncHelper jobSyncHelper;
    @Autowired
    private MeasureRepo<Measure> measureRepo;
    @Autowired
    private DataConnectorRepo dataConnectorRepo;

    @Override
    public Iterable<Measure> getAllAliveMeasures() {
        return measureRepo.findByDeleted(false);
    }

    @Override
    public Measure getMeasureById(long id) {
        return measureRepo.findByIdAndDeleted(id, false);
    }

    @Override
    public GriffinOperationMessage deleteMeasureById(Long measureId) {
        if (!measureRepo.exists(measureId)) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        }
        try {
            Measure measure = measureRepo.findOne(measureId);
            measure.setDeleted(true);
            measureRepo.save(measure);
            if (jobSyncHelper.deleteJobsRelateToMeasure(measure)) {
                return GriffinOperationMessage.DELETE_MEASURE_BY_ID_SUCCESS;
            }
            measure.setDeleted(false);
            measureRepo.save(measure);
        } catch (Exception e) {
            LOGGER.error("Failed to delete measure whose id is {} ", measureId);
        }
        return GriffinOperationMessage.DELETE_MEASURE_BY_ID_FAIL;
    }

    @Override
    public GriffinOperationMessage createMeasure(Measure measure) {
        List<Measure> aliveMeasureList = measureRepo.findByNameAndDeleted(measure.getName(), false);
        if (aliveMeasureList.size() != 0) {
            LOGGER.error("Failed to create new measure {}, it already exists.", measure.getName());
            return GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE;
        }
        if (measure instanceof GriffinMeasure) {
            return createGriffinMeasure((GriffinMeasure) measure);
        }
        return createExternalMeasure((ExternalMeasure) measure);
    }


    private GriffinOperationMessage createGriffinMeasure(GriffinMeasure measure) {
        if (!isConnectorNamesValid(measure)) {
            LOGGER.error("Failed to create new measure {}. Its connector names already exist. ", measure.getName());
            return GriffinOperationMessage.CREATE_MEASURE_FAIL;
        }
        try {
            measureRepo.save(measure);
            return GriffinOperationMessage.CREATE_MEASURE_SUCCESS;
        } catch (Exception e) {
            LOGGER.error("Failed to create new measure {}.{}", measure.getName(), e.getMessage());
        }
        return GriffinOperationMessage.CREATE_MEASURE_FAIL;
    }

    private GriffinOperationMessage createExternalMeasure(ExternalMeasure measure) {
        if (StringUtils.isBlank(measure.getMetricName())) {
            LOGGER.error("Failed to create new measure {}. Its metric name is blank.", measure.getName());
            return GriffinOperationMessage.CREATE_MEASURE_FAIL;
        }
        try {
            measure = measureRepo.save(measure);
            if (jobSyncHelper.createVirtualJob(measure)) {
                return GriffinOperationMessage.CREATE_MEASURE_SUCCESS;
            }
            measureRepo.delete(measure);
        } catch (Exception e) {
            LOGGER.error("Failed to create new measure {}.{}", measure.getName(), e.getMessage());
        }
        return GriffinOperationMessage.CREATE_MEASURE_FAIL;
    }

    private boolean isConnectorNamesValid(GriffinMeasure measure) {
        List<String> names = getConnectorNames(measure);
        List<DataConnector> connectors = dataConnectorRepo.findByConnectorNames(names);
        return names.size() != 0 && CollectionUtils.isEmpty(connectors);
    }

    private List<String> getConnectorNames(GriffinMeasure measure) {
        List<String> names = new ArrayList<>();
        for (DataSource source : measure.getDataSources()) {
            for (DataConnector dc : source.getConnectors()) {
                String name = dc.getName();
                if (!StringUtils.isEmpty(name)) {
                    names.add(name);
                }
            }
        }
        return names;
    }

    @Override
    public List<Measure> getAliveMeasuresByOwner(String owner) {
        return measureRepo.findByOwnerAndDeleted(owner, false);
    }

    @Override
    public GriffinOperationMessage updateMeasure(Measure measure) {
        Measure originMeasure = measureRepo.findByIdAndDeleted(measure.getId(), false);
        if (originMeasure == null) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        }
        if (!originMeasure.getType().equals(measure.getType())) {
            LOGGER.error("Can't update measure to different type.");
            return GriffinOperationMessage.UPDATE_MEASURE_FAIL;
        }
        if (measure instanceof GriffinMeasure) {
            return updateGriffinMeasure((GriffinMeasure) measure);
        }
        return updateExternalMeasure((ExternalMeasure) originMeasure, (ExternalMeasure) measure);
    }

    private GriffinOperationMessage updateGriffinMeasure(GriffinMeasure measure) {
        try {
            measureRepo.save(measure);
            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
        } catch (Exception e) {
            LOGGER.error("Failed to update measure. {}", e.getMessage());
        }
        return GriffinOperationMessage.UPDATE_MEASURE_FAIL;
    }

    private GriffinOperationMessage updateExternalMeasure(ExternalMeasure originMeasure, ExternalMeasure newMeasure) {
        try {
            if (jobSyncHelper.updateVirtualJob(newMeasure)) {
                measureRepo.save(newMeasure);
                return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to update measure. {}", e.getMessage());
        }
        jobSyncHelper.updateVirtualJob(originMeasure);
        return GriffinOperationMessage.UPDATE_MEASURE_FAIL;
    }
}
