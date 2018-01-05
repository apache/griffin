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
import org.apache.griffin.core.job.entity.VirtualJob;
import org.apache.griffin.core.job.repo.VirtualJobRepo;
import org.apache.griffin.core.measure.entity.ExternalMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.ExternalMeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("externalOperation")
public class ExternalMeasureOperationImpl implements MeasureOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalMeasureOperationImpl.class);

    @Autowired
    private ExternalMeasureRepo measureRepo;
    @Autowired
    private VirtualJobRepo jobRepo;

    @Override
    public GriffinOperationMessage create(Measure measure) {
        ExternalMeasure em = (ExternalMeasure) measure;
        if (StringUtils.isBlank(em.getMetricName())) {
            LOGGER.error("Failed to create external measure {}. Its metric name is blank.", measure.getName());
            return GriffinOperationMessage.CREATE_MEASURE_FAIL;
        }
        try {
            em.setVirtualJob(new VirtualJob());
            em = measureRepo.save(em);
            VirtualJob vj = genVirtualJob(em, em.getVirtualJob());
            jobRepo.save(vj);
            return GriffinOperationMessage.CREATE_MEASURE_SUCCESS;
        } catch (Exception e) {
            LOGGER.error("Failed to create new measure {}.{}", em.getName(), e.getMessage());
        }
        return GriffinOperationMessage.CREATE_MEASURE_FAIL;
    }

    @Override
    public GriffinOperationMessage update(Measure measure) {
        ExternalMeasure latestMeasure = (ExternalMeasure) measure;
        if (StringUtils.isBlank(latestMeasure.getMetricName())) {
            LOGGER.error("Failed to create external measure {}. Its metric name is blank.", measure.getName());
            return GriffinOperationMessage.UPDATE_MEASURE_FAIL;
        }
        try {
            ExternalMeasure originMeasure = measureRepo.findOne(latestMeasure.getId());
            VirtualJob vj = genVirtualJob(latestMeasure, originMeasure.getVirtualJob());
            latestMeasure.setVirtualJob(vj);
            measureRepo.save(latestMeasure);
            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
        } catch (Exception e) {
            LOGGER.error("Failed to update measure. {}", e.getMessage());
        }
        return GriffinOperationMessage.UPDATE_MEASURE_FAIL;
    }

    @Override
    public Boolean delete(Measure measure) {
        try {
            ExternalMeasure em = (ExternalMeasure) measure;
            em.setDeleted(true);
            em.getVirtualJob().setDeleted(true);
            measureRepo.save(em);
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to delete measure. {}", e.getMessage());
        }
        return false;

    }

    private VirtualJob genVirtualJob(ExternalMeasure em, VirtualJob vj) {
        vj.setMeasureId(em.getId());
        vj.setJobName(em.getName());
        vj.setMetricName(em.getMetricName());
        return vj;
    }
}
