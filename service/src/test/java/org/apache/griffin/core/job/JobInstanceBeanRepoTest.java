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

import org.apache.griffin.core.job.entity.JobInstanceBean;
import org.apache.griffin.core.job.entity.LivySessionStates;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.apache.griffin.core.job.entity.LivySessionStates.State.*;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.busy;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.running;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@PropertySource("classpath:application.properties")
@DataJpaTest
public class JobInstanceBeanRepoTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private JobInstanceRepo jobInstanceRepo;

    @Before
    public void setUp() {
        setEntityManager();
    }

//    @Test
//    public void testFindByJobIdWithPageable() {
//        Pageable pageRequest = new PageRequest(0, 10, Sort.Direction.DESC, "timestamp");
//        List<JobInstanceBean> instances = jobInstanceRepo.findByJobId(1L, pageRequest);
//        assertThat(instances.size()).isEqualTo(1);
//        assertEquals(instances.get(0).getAppId(), "appId1");
//    }


    @Test
    public void testFindByActiveState() {
        LivySessionStates.State[] states = {starting, not_started, recovering, idle, running, busy};
        List<JobInstanceBean> list = jobInstanceRepo.findByActiveState(states);
        assertThat(list.size()).isEqualTo(1);
    }




    private void setEntityManager() {
        JobInstanceBean instance1 = new JobInstanceBean(1L,  LivySessionStates.State.success,
                "appId1", "http://domain.com/uri1", System.currentTimeMillis(),System.currentTimeMillis());
        JobInstanceBean instance2 = new JobInstanceBean(2L,  LivySessionStates.State.error,
                "appId2", "http://domain.com/uri2", System.currentTimeMillis(),System.currentTimeMillis());
        JobInstanceBean instance3 = new JobInstanceBean(2L,  LivySessionStates.State.starting,
                "appId3", "http://domain.com/uri3", System.currentTimeMillis(),System.currentTimeMillis());
        entityManager.persistAndFlush(instance1);
        entityManager.persistAndFlush(instance2);
        entityManager.persistAndFlush(instance3);
    }
}
