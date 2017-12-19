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

import org.apache.griffin.core.job.entity.JobInstanceBean;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface JobInstanceRepo extends CrudRepository<JobInstanceBean, Long> {

    @Query("select s from JobInstanceBean s where s.jobId = ?1")
    List<JobInstanceBean> findByJobId(Long jobId, Pageable pageable);

    @Query("select DISTINCT s from JobInstanceBean s " +
            "where s.state in ('starting', 'not_started', 'recovering', 'idle', 'running', 'busy')")
    List<JobInstanceBean> findByActiveState();

    @Modifying
    @Query("delete from JobInstanceBean s ")
    void deleteByJobName(String jobName);

    @Modifying
    @Query("update JobInstanceBean s " +
            "set s.state= ?2, s.appId= ?3, s.appUri= ?4 where s.id= ?1")
    void update(Long id, LivySessionStates.State state, String appId, String appUri);

}
