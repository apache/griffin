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
package org.apache.griffin.core.job.repo;

import org.apache.griffin.core.job.entity.JobInstance;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface JobInstanceRepo extends CrudRepository<JobInstance, Long> {
    /**
     * @param group    is group name
     * @param name     is job name
     * @param pageable
     * @return all job instances scheduled at different time using the same prototype job,
     * the prototype job is determined by SCHED_NAME, group name and job name in table QRTZ_JOB_DETAILS.
     */
    @Query("select s from JobInstance s " +
            "where s.groupName= ?1 and s.jobName=?2 "/*+
            "order by s.timestamp desc"*/)
    List<JobInstance> findByGroupNameAndJobName(String group, String name, Pageable pageable);

    @Query("select s from JobInstance s " +
            "where s.groupName= ?1 and s.jobName=?2 ")
    List<JobInstance> findByGroupNameAndJobName(String group, String name);

    @Query("select DISTINCT s.groupName, s.jobName, s.id from JobInstance s")
    List<Object> findGroupWithJobName();

    @Modifying
    @Query("delete from JobInstance s " +
            "where s.groupName= ?1 and s.jobName=?2 ")
    void deleteByGroupAndJobName(String groupName, String jobName);

    @Modifying
    @Query("update JobInstance s " +
            "set s.state= ?2, s.appId= ?3, s.appUri= ?4 where s.id= ?1")
    void update(Long Id, LivySessionStates.State state, String appId, String appUri);

}
