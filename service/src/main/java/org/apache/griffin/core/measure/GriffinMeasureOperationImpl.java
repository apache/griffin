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
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.apache.griffin.core.util.GriffinOperationMessage.*;
import static org.apache.griffin.core.util.MeasureUtil.isValid;

@Component("griffinOperation")
public class GriffinMeasureOperationImpl implements MeasureOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(GriffinMeasureOperationImpl.class);

    @Autowired
    private MeasureRepo<Measure> measureRepo;

    @Autowired
    private JobServiceImpl jobService;


    @Override
    public GriffinOperationMessage create(Measure measure) {
        if (!isValid((GriffinMeasure) measure)) {
            return CREATE_MEASURE_FAIL;
        }
        try {
            measureRepo.save(measure);
            return CREATE_MEASURE_SUCCESS;
        } catch (Exception e) {
            LOGGER.error("Failed to create new measure {}.", measure.getName(), e);
        }
        return CREATE_MEASURE_FAIL;
    }

    @Override
    public GriffinOperationMessage update(Measure measure) {
        try {
            if (!isValid((GriffinMeasure) measure)) {
                return CREATE_MEASURE_FAIL;
            }
            measure.setDeleted(false);
            measureRepo.save(measure);
            return UPDATE_MEASURE_SUCCESS;
        } catch (Exception e) {
            LOGGER.error("Failed to update measure. {}", e.getMessage());
        }
        return UPDATE_MEASURE_FAIL;
    }

    @Override
    public GriffinOperationMessage delete(Measure measure) {
        try {
            boolean pauseStatus = jobService.deleteJobsRelateToMeasure(measure.getId());
            if (!pauseStatus) {
                return DELETE_MEASURE_BY_ID_FAIL;
            }
            measure.setDeleted(true);
            measureRepo.save(measure);
            return DELETE_MEASURE_BY_ID_SUCCESS;
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        return DELETE_MEASURE_BY_ID_FAIL;
    }
}
