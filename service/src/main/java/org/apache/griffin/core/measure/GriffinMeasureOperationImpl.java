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
import org.apache.griffin.core.job.JobServiceImpl;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.DataConnectorRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.griffin.core.util.GriffinOperationMessage.*;

@Component("griffinOperation")
public class GriffinMeasureOperationImpl implements MeasureOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(GriffinMeasureOperationImpl.class);

    @Autowired
    private MeasureRepo<Measure> measureRepo;
    @Autowired
    private DataConnectorRepo dcRepo;
    @Autowired
    private JobServiceImpl jobService;


    @Override
    public GriffinOperationMessage create(Measure measure) {
        if (!isConnectorNamesValid((GriffinMeasure) measure)) {
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
    public Boolean delete(Measure measure) {
        boolean pauseStatus = jobService.deleteJobsRelateToMeasure(measure.getId());
        if (!pauseStatus) {
            return false;
        }
        measure.setDeleted(true);
        measureRepo.save(measure);
        return true;
    }

    private boolean isConnectorNamesValid(GriffinMeasure measure) {
        List<String> names = getConnectorNames(measure);
        if (names.size() == 0) {
            LOGGER.warn("Connector names cannot be empty.");
            return false;
        }
        List<DataConnector> connectors = dcRepo.findByConnectorNames(names);
        if (!CollectionUtils.isEmpty(connectors)) {
            LOGGER.warn("Failed to create new measure {}. It's connector names already exist. ", measure.getName());
            return false;
        }
        return true;
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
}
