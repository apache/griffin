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
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.griffin.core.util.GriffinOperationMessage.*;

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

    private boolean isValid(GriffinMeasure measure) {
        if (!isConnectorNamesValid(measure)) {
            return false;
        }
        return true;
    }

    private boolean isConnectorNamesValid(GriffinMeasure measure) {
        Set<String> sets = new HashSet<>();
        List<DataSource> sources = measure.getDataSources();
        for (DataSource source : sources) {
            source.getConnectors().stream().filter(dc -> dc.getName() != null).forEach(dc -> sets.add(dc.getName()));
        }
        if (sets.size() == 0 || sets.size() < sources.size()) {
            LOGGER.warn("Connector names cannot be repeated or empty.");
            return false;
        }
        return true;
    }
}
