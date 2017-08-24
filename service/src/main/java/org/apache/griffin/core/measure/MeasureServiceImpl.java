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


import org.apache.griffin.core.job.SparkSubmitJob;
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

    /*
        TODO: require to be fixed: deleting measure doesn't deal with job protocol related to it, leading quartz to throw error that measure cannot be found.
     */
    @Override
    public GriffinOperationMessage deleteMeasureById(@PathVariable("MeasureId") Long measureId) {
        if (measureRepo.exists(measureId) == false) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            //pause all jobs related to the measure
            Measure measure = measureRepo.findOne(measureId);
            pauseJobs(measure);
            measure.setDeleted(true);
            measureRepo.save(measure);
            return GriffinOperationMessage.DELETE_MEASURE_BY_ID_SUCCESS;
        }
    }

    private void pauseJobs(Measure measure) {

        Scheduler scheduler = factory.getObject();
        try {
            for(JobKey jobKey: scheduler.getJobKeys(GroupMatcher.anyGroup())){//get all jobs
                JobDetail jobDetail = scheduler.getJobDetail(jobKey);
                JobDataMap jobDataMap = jobDetail.getJobDataMap();
                if(jobDataMap.getString("measureId").equals(measure.getId().toString())){//select jobs related to measureId
                    scheduler.pauseJob(jobKey);
                    jobDataMap.putAsString("deleted", true);
                    scheduler.addJob(jobDetail, true);
                    log.info(jobKey.getGroup()+" "+jobKey.getName()+" is paused and logically deleted.");
                }
            }
        } catch (SchedulerException e) {
            log.error("Fail to stop jobs related to measure id: " + measure.getId()+"name: "+measure.getName());
        }
    }

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
        for(Measure measure: measureRepo.findByOwnerAndDeleted(owner, false)){
            HashMap<String, String> map = new HashMap<>();
            map.put("name", measure.getName());
            map.put("id", measure.getId().toString());
            res.add(map);
        }
        return res;
    }

    public GriffinOperationMessage updateMeasure(@RequestBody Measure measure) {
        if (measureRepo.exists(measure.getId()) == false) {
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        } else {
            measureRepo.save(measure);
            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
        }
    }
}
