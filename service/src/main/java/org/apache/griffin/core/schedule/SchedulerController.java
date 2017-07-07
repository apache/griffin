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

package org.apache.griffin.core.schedule;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.Serializable;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/jobs")
public class SchedulerController {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerController.class);

    @Autowired
    SchedulerService schedulerService;

    @RequestMapping("/")
    public List<Map<String, Serializable>> getJobs() throws SchedulerException,
            ParseException {
        return schedulerService.getJobs();
    }

    @RequestMapping(value = "/add/{groupName}/{jobName}/{measureName}", method = RequestMethod.POST)
    @ResponseBody
    @Produces(MediaType.APPLICATION_JSON)
    public boolean addJob(@PathVariable String groupName,
                           @PathVariable String jobName,
                           @PathVariable String measureName,
                           @RequestBody SchedulerRequestBody schedulerRequestBody) {
        return schedulerService.addJob(groupName,jobName,measureName,schedulerRequestBody);
    }

    @RequestMapping(value = "/del/{group}/{name}", method = RequestMethod.DELETE)
    public boolean deleteJob(@PathVariable String group, @PathVariable String name) {
        return schedulerService.deleteJob(group,name);
    }

    @RequestMapping("/instances/{group}/{jobName}/{page}/{size}")
    public List<ScheduleState> findInstancesOfJob(@PathVariable String group,@PathVariable String jobName,
                                                  @PathVariable int page,@PathVariable int size){
        return schedulerService.findInstancesOfJob(group,jobName,page,size);
    }

    @RequestMapping("/statics")
    public JobHealth getHealthInfo() throws SchedulerException {
        return schedulerService.getHealthInfo();
    }
}


