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
import org.apache.griffin.core.job.repo.JobRepo;
import org.apache.griffin.core.measure.entity.ExternalMeasure;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ExternalMeasureOperationImpl implements MeasureOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalMeasureOperationImpl.class);

    @Autowired
    private MeasureRepo<Measure> measureRepo;
    @Autowired
    private JobRepo<VirtualJob> jobRepo;

    @Override
    public GriffinOperationMessage create(Measure measure) {
        String metricName = ((ExternalMeasure) measure).getMetricName();
        if (StringUtils.isBlank(metricName)) {
            LOGGER.error("Failed to create external measure {}. Its metric name is blank.", measure.getName());
            return GriffinOperationMessage.CREATE_MEASURE_FAIL;
        }
        try {
            measure = measureRepo.save(measure);
            if (createRelatedVirtualJob((ExternalMeasure) measure)) {
                return GriffinOperationMessage.CREATE_MEASURE_SUCCESS;
            }
            measureRepo.delete(measure);
        } catch (Exception e) {
            LOGGER.error("Failed to create new measure {}.{}", measure.getName(), e.getMessage());
        }
        return GriffinOperationMessage.CREATE_MEASURE_FAIL;
    }

    @Override
    public GriffinOperationMessage update(Measure measure) {
        try {
            if (updateRelatedVirtualJob((ExternalMeasure) measure)) {
                measureRepo.save(measure);
                return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to update measure. {}", e.getMessage());
        }
        return GriffinOperationMessage.UPDATE_MEASURE_FAIL;
    }

    @Override
    public Boolean delete(Long id) {
        List<VirtualJob> jobList = jobRepo.findByMeasureIdAndDeleted(id, false);
        switch (jobList.size()) {
            case 1:
                VirtualJob job = jobList.get(0);
                job.setDeleted(true);
                jobRepo.save(job);
                LOGGER.info("Virtual job {} is logically deleted.", job.getJobName());
                return true;
            case 0:
                LOGGER.error("Can't find the virtual job related to measure id {}.", id);
                return false;
            default:
                LOGGER.error("More than one virtual job related to measure id {} found.", id);
                return false;
        }
    }

    private Boolean createRelatedVirtualJob(ExternalMeasure measure) {
        if (jobRepo.findByMeasureIdAndDeleted(measure.getId(), false).size() != 0) {
            LOGGER.error("Failed to create new virtual job related to measure {}, it already exists.", measure.getName());
            return false;
        }
        if (jobRepo.findByJobNameAndDeleted(measure.getName(), false).size() != 0) {
            LOGGER.error("Failed to create new virtual job {}, it already exists.", measure.getName());
            return false;
        }
        VirtualJob job = new VirtualJob(measure.getName(), measure.getId(), measure.getMetricName());
        try {
            jobRepo.save(job);
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to save virtual job {}. {}", measure.getName(), e.getMessage());
        }
        return false;
    }

    private Boolean updateRelatedVirtualJob(ExternalMeasure measure) {
        List<VirtualJob> jobList = jobRepo.findByMeasureIdAndDeleted(measure.getId(), false);
        switch (jobList.size()) {
            case 1:
                VirtualJob job = jobList.get(0);
                job.setJobName(measure.getName());
                job.setMetricName(measure.getMetricName());
                jobRepo.save(job);
                LOGGER.info("Virtual job {} is updated.", job.getJobName());
                return true;
            case 0:
                LOGGER.error("Can't find the virtual job related to measure id {}.", measure.getId());
                return false;
            default:
                LOGGER.error("More than one virtual job related to measure id {} found.", measure.getId());
                return false;
        }

    }
}
