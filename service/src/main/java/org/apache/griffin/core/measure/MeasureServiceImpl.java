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


import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import javax.print.attribute.HashAttributeSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MeasureServiceImpl implements MeasureService {
    private static final Logger log = LoggerFactory.getLogger(MeasureServiceImpl.class);

    @Autowired
    MeasureRepo measureRepo;

    @Override
    public Iterable<Measure> getAllMeasures() {
        return measureRepo.findAll();
    }

    @Override
    public Measure getMeasureById(@PathVariable("id") long id) {
        return measureRepo.findOne(id);
    }

    @Override
    public Measure getMeasureByName(@PathVariable("measureName") String measureName) {
        return measureRepo.findByName(measureName);
    }

    /*
        TODO: require to be fixed: deleting measure doesn't deal with job protocol related to it, leading quartz to throw error that measure cannot be found.
     */
    @Override
    public GriffinOperationMessage deleteMeasureById(@PathVariable("MeasureId") Long MeasureId) {
        Measure temp_mesaure = measureRepo.findOne(MeasureId);
        if (temp_mesaure == null) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            measureRepo.delete(MeasureId);
            return GriffinOperationMessage.DELETE_MEASURE_BY_ID_SUCCESS;
        }
    }

    @Override
    public GriffinOperationMessage deleteMeasureByName(@PathVariable("measureName") String measureName) {
        Measure temp_mesaure = measureRepo.findByName(measureName);
        if (temp_mesaure == null) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            measureRepo.delete(temp_mesaure.getId());
            return GriffinOperationMessage.DELETE_MEASURE_BY_NAME_SUCCESS;
        }
    }

    @Override
    public GriffinOperationMessage createMeasure(@RequestBody Measure measure) {
        String name = measure.getName();
        Measure temp_mesaure = measureRepo.findByName(name);
        if (temp_mesaure == null) {
            if (measureRepo.save(measure) != null)
                return GriffinOperationMessage.CREATE_MEASURE_SUCCESS;
            else {
                return GriffinOperationMessage.CREATE_MEASURE_FAIL;
            }
        } else {
            log.info("Failed to create new measure " + name + ", it already exists");
            return GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE;
        }
    }

    @Override
    public List<Map<String, String>> getAllMeasureByOwner(String owner) {
        List<Map<String, String>> res = new ArrayList<>();
        for (Measure measure : measureRepo.findAll()) {
            if (measure.getOwner().equals(owner)) {
                HashMap<String, String> map = new HashMap<>();
                map.put("name", measure.getName());
                map.put("id", measure.getId().toString());
                res.add(map);
            }
        }
        return res;
    }

    public GriffinOperationMessage updateMeasure(@RequestBody Measure measure) {
//        Long measureId=measure.getId();
//        if (measureRepo.findOne(measureId)==null){
//            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
//        }else{
//            measureRepo.updateMeasure(measureId,measure.getDescription(),measure.getOrganization(),measure.getSource(),measure.getTarget(),measure.getEvaluateRule());
////            System.out.print(res);
//            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
//        }
        String name = measure.getName();
        Measure temp_mesaure = measureRepo.findByName(name);
        if (temp_mesaure == null) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            //in this way, id will changed
            //TODO, FRONTEND ID?
            measureRepo.delete(temp_mesaure.getId());
            measureRepo.save(measure);
            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
        }
    }
}
