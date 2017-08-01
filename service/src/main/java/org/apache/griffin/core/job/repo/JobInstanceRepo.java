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

import org.apache.griffin.core.job.entity.LivySessionStateMap;
import org.apache.griffin.core.job.entity.JobInstance;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;


@Repository
public interface JobInstanceRepo extends CrudRepository<JobInstance,Long>{
    @Query("select s from JobInstance s " +
            "where s.groupName= ?1 and s.jobName=?2 "/*+
            "order by s.timestamp desc"*/)
    List<JobInstance> findByGroupNameAndJobName(String group, String name, Pageable pageable);

    @Query("select s from JobInstance s " +
            "where s.groupName= ?1 and s.jobName=?2 ")
    List<JobInstance> findByGroupNameAndJobName(String group, String name);

    @Query("select DISTINCT s.groupName, s.jobName from JobInstance s")
    List<Object> findGroupWithJobName();

    @Transactional
    @Modifying
    @Query("update JobInstance s "+
            "set s.state= ?2, s.appId= ?3 where s.id= ?1")
    void setFixedStateAndappIdFor(Long Id, LivySessionStateMap.State state, String appId);

    @Transactional
    @Modifying
    @Query("delete from JobInstance s " +
            "where s.groupName= ?1 and s.jobName=?2 ")
    void deleteInGroupAndjobName(String groupName, String jobName);


}
