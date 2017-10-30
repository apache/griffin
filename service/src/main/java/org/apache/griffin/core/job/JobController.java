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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.JobRequestBody;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Api(tags = "Jobs",description = "execute your measure periodically")
@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobController.class);

    @Autowired
    private JobService jobService;

    @ApiOperation(value = "Get jobs", response = List.class)
    @RequestMapping(value = "", method = RequestMethod.GET)
    public List<Map<String, Serializable>> getJobs() {
        return jobService.getAliveJobs();
    }

    @ApiOperation(value = "Add job", response = GriffinOperationMessage.class)
    @RequestMapping(value = "", method = RequestMethod.POST)
    public GriffinOperationMessage addJob(@ApiParam(value = "job group name", required = true) @RequestParam("group") String groupName,
                                          @ApiParam(value = "job name", required = true)  @RequestParam("jobName") String jobName,
                                          @ApiParam(value = "measure id, required = true") @RequestParam("measureId") Long measureId,
                                          @ApiParam(value = "custom class composed of job key parameters", required = true)
                                              @RequestBody JobRequestBody jobRequestBody) {
        return jobService.addJob(groupName, jobName, measureId, jobRequestBody);
    }

    @ApiOperation(value = "Delete job", response = GriffinOperationMessage.class)
    @RequestMapping(value = "", method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteJob(@ApiParam(value = "job group name", required = true) @RequestParam("group") String group,
                                             @ApiParam(value = "job name", required = true) @RequestParam("jobName") String jobName) {
        return jobService.deleteJob(group, jobName);
    }

    @ApiOperation(value = "Get job instances", response = List.class)
    @RequestMapping(value = "/instances", method = RequestMethod.GET)
    public List<JobInstance> findInstancesOfJob(@ApiParam(value = "job group name", required = true) @RequestParam("group") String group,
                                                @ApiParam(value = "job name", required = true) @RequestParam("jobName") String jobName,
                                                @ApiParam(value = "page you want starting from index 0", required = true) @RequestParam("page") int page,
                                                @ApiParam(value = "instance number per page", required = true) @RequestParam("size") int size) {
        return jobService.findInstancesOfJob(group, jobName, page, size);
    }

    @ApiOperation(value = "Get job healthy statistics", response = JobHealth.class)
    @RequestMapping(value = "/health", method = RequestMethod.GET)
    public JobHealth getHealthInfo() {
        return jobService.getHealthInfo();
    }
}


