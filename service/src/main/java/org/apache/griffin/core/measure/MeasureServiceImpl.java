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
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MeasureServiceImpl implements MeasureService {
    private static final Logger log = LoggerFactory.getLogger(MeasureServiceImpl.class);

    @Autowired
    private MeasureRepo measureRepo;
    @Autowired
    private SchedulerFactoryBean factory;
    @Override
    public Iterable<Measure> getAllAliveMeasures() {
        return measureRepo.findByDeleted(false);
    }

    @Override
    public Measure getMeasureById(@PathVariable("id") long id) {
        return measureRepo.findOne(id);
    }

    /*@Override
    public Measure getMeasureByName(@PathVariable("measureName") String measureName) {
        return measureRepo.findByName(measureName);
    }*/

    /*
        TODO: require to be fixed: deleting measure doesn't deal with job protocol related to it, leading quartz to throw error that measure cannot be found.
     */
    @Override
    public GriffinOperationMessage deleteMeasureById(@PathVariable("MeasureId") Long measureId) {
        if (measureRepo.exists(measureId) == false) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            Measure measure = measureRepo.findOne(measureId);
            measure.setDeleted(true);
            Measure save = measureRepo.save(measure);
            //stop all jobs related to the measure
            stopJobs(measureId);
            return GriffinOperationMessage.DELETE_MEASURE_BY_ID_SUCCESS;
        }
    }

    private void stopJobs(Long measureId) {
        Scheduler scheduler = factory.getObject();
        try {
            for(JobKey jobKey: scheduler.getJobKeys(GroupMatcher.anyGroup())){//get all jobs
                JobDataMap jobDataMap = scheduler.getJobDetail(jobKey).getJobDataMap();
                if(jobDataMap.getString("measureId").equals(measureId.toString())){//select jobs related to measureId
                    List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);//get all triggers of one job
                    for (Trigger trigger: triggers){//unschedule all triggers
                        scheduler.unscheduleJob(trigger.getKey());
                    }
                }


            }
        } catch (SchedulerException e) {
            log.error("Fail to stop jobs related to measure " + measureId);
        }
    }

  /*  @Override
    public GriffinOperationMessage deleteMeasureByName(@PathVariable("measureName") String measureName) {
        Measure temp_mesaure = measureRepo.findByName(measureName);
        if (temp_mesaure == null) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            measureRepo.delete(temp_mesaure.getId());
            return GriffinOperationMessage.DELETE_MEASURE_BY_NAME_SUCCESS;
        }
    }*/

    @Override
    public GriffinOperationMessage createMeasure(@RequestBody Measure measure) {
        List<Measure> aliveMeasureList = measureRepo.findByNameAndDeleted(measure.getName(), false);
        if (aliveMeasureList.size() == 0) {
            if (measureRepo.save(measure) != null)
                return GriffinOperationMessage.CREATE_MEASURE_SUCCESS;
            else {
                return GriffinOperationMessage.CREATE_MEASURE_FAIL;
            }
        } else {
            log.info("Failed to create new measure " + measure.getName() + ", it already exists");
            return GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE;
        }
    }

    @Override
    public List<Map<String, String>> getAllAliveMeasureNameIdByOwner(String owner) {
        List<Map<String, String>> res = new ArrayList<>();
        /*for (Measure measure : measureRepo.findAll()) {
            if (measure.getOwner().equals(owner)) {
                HashMap<String, String> map = new HashMap<>();
                map.put("name", measure.getName());
                map.put("id", measure.getId().toString());
                res.add(map);
            }
        }*/
        for(Measure measure: measureRepo.findByOwnerAndDeleted(owner, false)){
            HashMap<String, String> map = new HashMap<>();
            map.put("name", measure.getName());
            map.put("id", measure.getId().toString());
            res.add(map);
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

        if (measureRepo.exists(measure.getId()) == false) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            measureRepo.save(measure);
            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
        }
    }
}
