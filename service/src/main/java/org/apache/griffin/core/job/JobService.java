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

import org.apache.griffin.core.job.entity.*;
import org.apache.griffin.core.measure.entity.GriffinMeasure.ProcessType;

import java.util.List;

public interface JobService {

    List<JobDataBean> getAliveJobs(String type);

    JobSchedule getJobSchedule(String jobName);

    JobSchedule getJobSchedule(Long jobId);

    JobSchedule addJob(JobSchedule js) throws Exception;

    JobSchedule onAction(Long jobId,String action);

    void deleteJob(Long jobId);

    void deleteJob(String jobName);

    List<JobInstanceBean> findInstancesOfJob(Long jobId, int page, int size);

    JobHealth getHealthInfo();
}
