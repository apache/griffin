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
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MeasureServiceImpl implements MeasureService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MeasureServiceImpl.class);

    @Autowired
    private JobServiceImpl jobService;
    @Autowired
    private MeasureRepo measureRepo;

    @Override
    public Iterable<Measure> getAllAliveMeasures() {
        return measureRepo.findByDeleted(false);
    }

    @Override
    public Measure getMeasureById(@PathVariable("id") long id) {
        return measureRepo.findOne(id);
    }

    @Override
    public GriffinOperationMessage deleteMeasureById(Long measureId) {
        if (!measureRepo.exists(measureId)) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            Measure measure = measureRepo.findOne(measureId);
            try{
                //pause all jobs related to the measure
                jobService.deleteJobsRelateToMeasure(measure);
                measure.setDeleted(true);
                measureRepo.save(measure);
            }catch (SchedulerException e){
                LOGGER.error("Delete measure id: {} name: {} failure. {}", measure.getId(), measure.getName(),e.getMessage());
                return GriffinOperationMessage.DELETE_MEASURE_BY_ID_FAIL;
            }

            return GriffinOperationMessage.DELETE_MEASURE_BY_ID_SUCCESS;
        }
    }

    @Override
    public GriffinOperationMessage createMeasure(Measure measure) {
        List<Measure> aliveMeasureList = measureRepo.findByNameAndDeleted(measure.getName(), false);
        if (aliveMeasureList.size() == 0) {
            try {
                if (measureRepo.save(measure) != null)
                    return GriffinOperationMessage.CREATE_MEASURE_SUCCESS;
                else {
                    return GriffinOperationMessage.CREATE_MEASURE_FAIL;
                }
            } catch (Exception e) {
                LOGGER.info("Failed to create new measure {}.{}", measure.getName(), e.getMessage());
                return GriffinOperationMessage.CREATE_MEASURE_FAIL;
            }

        } else {
            LOGGER.info("Failed to create new measure {}, it already exists.", measure.getName());
            return GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE;
        }
    }

    @Override
    public List<Map<String, String>> getAllAliveMeasureNameIdByOwner(String owner) {
        List<Map<String, String>> res = new ArrayList<>();
        for (Measure measure : measureRepo.findByOwnerAndDeleted(owner, false)) {
            HashMap<String, String> map = new HashMap<>();
            map.put("name", measure.getName());
            map.put("id", measure.getId().toString());
            res.add(map);
        }
        return res;
    }

    public GriffinOperationMessage updateMeasure(@RequestBody Measure measure) {
        if (!measureRepo.exists(measure.getId())) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            try {
                measureRepo.save(measure);
            } catch (Exception e) {
                LOGGER.error("Failed to update measure. {}", e.getMessage());
                return GriffinOperationMessage.UPDATE_MEASURE_FAIL;
            }

            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
        }
    }
}
