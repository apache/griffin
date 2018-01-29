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


import org.apache.griffin.core.exception.GriffinException;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

import static org.apache.griffin.core.exception.GriffinExceptionMessage.*;

@Service
public class MeasureServiceImpl implements MeasureService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MeasureServiceImpl.class);

    @Autowired
    private MeasureRepo<Measure> measureRepo;
    @Autowired
    @Qualifier("griffinOperation")
    private MeasureOperation griffinOp;
    @Autowired
    @Qualifier("externalOperation")
    private MeasureOperation externalOp;

    @Override
    public List<Measure> getAllAliveMeasures() {
        return measureRepo.findByDeleted(false);
    }

    @Override
    public Measure getMeasureById(long id) {
        Measure measure = measureRepo.findByIdAndDeleted(id, false);
        if (measure == null) {
            throw new GriffinException.NotFoundException(MEASURE_ID_DOES_NOT_EXIST);
        }
        return measure;
    }

    @Override
    public List<Measure> getAliveMeasuresByOwner(String owner) {
        return measureRepo.findByOwnerAndDeleted(owner, false);
    }

    @Override
    public Measure createMeasure(Measure measure) {
        List<Measure> aliveMeasureList = measureRepo.findByNameAndDeleted(measure.getName(), false);
        if (!CollectionUtils.isEmpty(aliveMeasureList)) {
            LOGGER.warn("Failed to create new measure {}, it already exists.", measure.getName());
            throw new GriffinException.ConflictException(MEASURE_NAME_ALREADY_EXIST);
        }
        MeasureOperation op = getOperation(measure);
        return op.create(measure);
    }

    @Override
    public void updateMeasure(Measure measure) {
        Measure m = measureRepo.findByIdAndDeleted(measure.getId(), false);
        if (m == null) {
            throw new GriffinException.NotFoundException(MEASURE_ID_DOES_NOT_EXIST);
        }
        if (!m.getType().equals(measure.getType())) {
            LOGGER.warn("Can't update measure to different type.");
            throw new GriffinException.BadRequestException(MEASURE_TYPE_DOES_NOT_MATCH);
        }
        MeasureOperation op = getOperation(measure);
        op.update(measure);
    }

    @Override
    public void deleteMeasureById(Long measureId) {
        Measure measure = measureRepo.findByIdAndDeleted(measureId, false);
        if (measure == null) {
            throw new GriffinException.NotFoundException(MEASURE_ID_DOES_NOT_EXIST);
        }
        MeasureOperation op = getOperation(measure);
        op.delete(measure);
    }

    private MeasureOperation getOperation(Measure measure) {
        if (measure instanceof GriffinMeasure) {
            return griffinOp;
        }
        return externalOp;
    }

}
