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
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;


@Repository
public interface JobInstanceRepo extends CrudRepository<JobInstanceBean, Long> {

    @Query("select DISTINCT s from JobInstanceBean s " +
            "where s.state in ('starting', 'not_started', 'recovering', 'idle', 'running', 'busy')")
    List<JobInstanceBean> findByActiveState();

    JobInstanceBean findByPredicateJobName(String name);

    @Query("select s from JobInstanceBean s where job_id = ?1 and s.deleted = ?2")
    List<JobInstanceBean> findByJobIdAndDeleted(Long jobId, Boolean deleted, Pageable pageable);

    List<JobInstanceBean> findByExpireTmsLessThanEqualAndDeleted(Long expireTms, Boolean deleted);

    @Transactional
    @Modifying
    @Query("delete from JobInstanceBean j where j.expireTms <= ?1")
    int deleteByExpireTimestamp(Long expireTms);

}
