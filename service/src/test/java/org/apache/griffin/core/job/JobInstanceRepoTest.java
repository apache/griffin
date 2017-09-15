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

import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.measure.MeasureRepoTest;
import org.apache.griffin.core.measure.repo.DataConnectorRepo;
import org.apache.griffin.core.measure.repo.EvaluateRuleRepo;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@PropertySource("classpath:application.properties")
@DataJpaTest
@Sql("classpath:test.sql")
public class JobInstanceRepoTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobInstanceRepoTest.class);


    @Autowired
    private TestEntityManager testEntityManager;

    @Autowired
    private JobInstanceRepo jobInstanceRepo;

    @Test
    public void testFindByGroupNameAndJobName3Args(){
/*
        jobInstanceRepo.findByGroupNameAndJobName();
*/

    }

}
