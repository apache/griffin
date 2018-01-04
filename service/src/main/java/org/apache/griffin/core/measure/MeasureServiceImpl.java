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


import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Service
public class MeasureServiceImpl implements MeasureService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MeasureServiceImpl.class);

    @Autowired
    private MeasureRepo<Measure> measureRepo;
    @Autowired
    private GriffinMeasureOperationImpl griffinOp;
    @Autowired
    private ExternalMeasureOperationImpl externalOp;

    @Override
    public Iterable<Measure> getAllAliveMeasures() {
        return measureRepo.findByDeleted(false);
    }

    @Override
    public Measure getMeasureById(long id) {
        return measureRepo.findByIdAndDeleted(id, false);
    }

    @Override
    public List<Measure> getAliveMeasuresByOwner(String owner) {
        return measureRepo.findByOwnerAndDeleted(owner, false);
    }

    @Override
    public GriffinOperationMessage createMeasure(Measure measure) {
        List<Measure> aliveMeasureList = measureRepo.findByNameAndDeleted(measure.getName(), false);
        if (!CollectionUtils.isEmpty(aliveMeasureList)) {
            LOGGER.warn("Failed to create new measure {}, it already exists.", measure.getName());
            return GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE;
        }
        MeasureOperation op = getOperation(measure);
        return op.create(measure);
    }

    @Override
    public GriffinOperationMessage updateMeasure(Measure measure) {
        Measure m = measureRepo.findByIdAndDeleted(measure.getId(), false);
        if (m == null) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        }
        if (!m.getType().equals(measure.getType())) {
            LOGGER.error("Can't update measure to different type.");
            return GriffinOperationMessage.UPDATE_MEASURE_FAIL;
        }
        MeasureOperation op = getOperation(measure);
        return op.update(measure);
    }

    @Override
    public GriffinOperationMessage deleteMeasureById(Long measureId) {
        Measure measure = measureRepo.findByIdAndDeleted(measureId, false);
        if (measure == null) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        }
        try {
            MeasureOperation op = getOperation(measure);
            if (op.delete(measureId)) {
                measure.setDeleted(true);
                measureRepo.save(measure);
                return GriffinOperationMessage.DELETE_MEASURE_BY_ID_SUCCESS;
            }

        } catch (Exception e) {
            LOGGER.error("Delete measure id: {} name: {} failure. {}", measure.getId(), measure.getName(), e.getMessage());
        }
        return GriffinOperationMessage.DELETE_MEASURE_BY_ID_FAIL;
    }

    private MeasureOperation getOperation(Measure measure) {
        if (measure instanceof GriffinMeasure) {
            return griffinOp;
        }
        return externalOp;
    }

}
