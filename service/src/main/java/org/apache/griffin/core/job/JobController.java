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

import org.apache.griffin.core.job.entity.JobDataBean;
import org.apache.griffin.core.job.entity.JobHealth;
import org.apache.griffin.core.job.entity.JobInstanceBean;
import org.apache.griffin.core.job.entity.JobSchedule;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class JobController {

    @Autowired
    private JobService jobService;

    @RequestMapping(value = "/jobs", method = RequestMethod.GET)
    public List<JobDataBean> getJobs() {
        return jobService.getAliveJobs();
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.POST)
    public GriffinOperationMessage addJob(@RequestBody JobSchedule jobSchedule) throws Exception {
        return jobService.addJob(jobSchedule);
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteJob(@RequestParam("jobName") String jobName) {
        return jobService.deleteJob(jobName);
    }

    @RequestMapping(value = "/jobs/{id}", method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteJob(@PathVariable("id") Long id) {
        return jobService.deleteJob(id);
    }

    @RequestMapping(value = "/jobs/instances", method = RequestMethod.GET)
    public List<JobInstanceBean> findInstancesOfJob(@RequestParam("jobId") Long id, @RequestParam("page") int page, @RequestParam("size") int size) {
        return jobService.findInstancesOfJob(id, page, size);
    }

    @RequestMapping(value = "/jobs/health", method = RequestMethod.GET)
    public JobHealth getHealthInfo() {
        return jobService.getHealthInfo();
    }
}


