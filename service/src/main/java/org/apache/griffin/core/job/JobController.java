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

package org.apache.griffin.core.job;

import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.JobRequestBody;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/jobs")
public class JobController {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobController.class);

    @Autowired
    private JobService jobService;

    @RequestMapping("/")
    public List<Map<String, Serializable>> getJobs() {
        return jobService.getJobs();
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @Produces(MediaType.APPLICATION_JSON)
    public GriffinOperationMessage addJob(@RequestParam("group") String groupName,
                                           @RequestParam("jobName") String jobName,
                                           @RequestParam("measureName") String measureName,
                                           @RequestBody JobRequestBody jobRequestBody) {
        return jobService.addJob(groupName,jobName,measureName, jobRequestBody);
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteJob(@RequestParam("group") String group, @RequestParam("jobName") String jobName) {
        return jobService.deleteJob(group,jobName);
    }

    @RequestMapping(value = "/instances",method = RequestMethod.GET)
    public List<JobInstance> findInstancesOfJob(@RequestParam("group") String group, @RequestParam("jobName") String jobName,
                                                @RequestParam("page") int page, @RequestParam("size") int size) {
        return jobService.findInstancesOfJob(group,jobName,page,size);
    }

    @RequestMapping(value = "/health",method = RequestMethod.GET)
    public JobHealth getHealthInfo()  {
        return jobService.getHealthInfo();
    }
}


