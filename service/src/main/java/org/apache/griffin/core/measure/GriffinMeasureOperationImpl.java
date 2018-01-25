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
import org.apache.griffin.core.exception.GriffinException;
import org.apache.griffin.core.job.JobServiceImpl;
import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.DataConnectorRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.griffin.core.exception.GriffinExceptionMessage.INVALID_CONNECTOR_NAME;

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
    public Measure create(Measure measure) {
        checkConnectorNames((GriffinMeasure) measure);
        return measureRepo.save(measure);
    }

    @Override
    public void update(Measure measure) {
        measureRepo.save(measure);
    }

    @Override
    public void delete(Measure measure) {
        jobService.deleteJobsRelateToMeasure(measure.getId());
        measure.setDeleted(true);
        measureRepo.save(measure);
    }

    private void checkConnectorNames(GriffinMeasure measure) {
        List<String> names = getConnectorNames(measure);
        if (names.size() == 0) {
            LOGGER.warn("Connector names cannot be empty.");
            throw new GriffinException.BadRequestException(INVALID_CONNECTOR_NAME);
        }
        List<DataConnector> connectors = dcRepo.findByConnectorNames(names);
        if (!CollectionUtils.isEmpty(connectors)) {
            LOGGER.warn("Failed to create new measure {}. It's connector names already exist. ", measure.getName());
            throw new GriffinException.BadRequestException(INVALID_CONNECTOR_NAME);
        }
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
