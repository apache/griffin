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

import org.apache.griffin.core.interceptor.Token;
import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class JobController {

    @Autowired
    private JobService jobService;

    @RequestMapping(value = "/jobs", method = RequestMethod.GET)
    public List<JobDataBean> getJobs(@RequestParam(value = "type", defaultValue = "") String type) {
        return jobService.getAliveJobs(type);
    }

    @RequestMapping(value = "/jobs/config/{jobName}")
    public JobSchedule getJobSchedule(@PathVariable("jobName") String jobName) {
        return jobService.getJobSchedule(jobName);
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.CREATED)
    @Token
    public AbstractJob addJob(@RequestBody JobSchedule jobSchedule) throws Exception {
        return jobService.addJob(jobSchedule);
    }

    @RequestMapping(value = "/jobs/up/{id}", method = RequestMethod.PUT)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Token
    public void startJob(@PathVariable("id") Long jobId) {
        jobService.startJob(jobId);
    }

    @RequestMapping(value = "/jobs/down/{id}", method = RequestMethod.PUT)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Token
    public void stopJob(@PathVariable("id") Long jobId) {
        jobService.stopJob(jobId);
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteJob(@RequestParam("jobName") String jobName) {
        jobService.deleteJob(jobName);
    }

    @RequestMapping(value = "/jobs/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteJob(@PathVariable("id") Long id) {
        jobService.deleteJob(id);
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


