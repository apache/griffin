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

package org.apache.griffin.core.measure.repo;

import org.apache.griffin.core.measure.entity.Measure;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.apache.griffin.core.measure.MeasureTestHelper.createATestMeasure;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@DataJpaTest
public class MeasureRepoTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private MeasureRepo measureRepo;

    @Before
    public void setup() throws Exception {
        entityManager.clear();
        entityManager.flush();
        setEntityManager();
    }

    @Test
    public void testFindAllOrganizations() {
        List<String> orgs = measureRepo.findOrganizations(false);
        assertThat(orgs.size()).isEqualTo(3);
    }


    @Test
    public void testFindNameByOrganization() {
        List<String> orgs = measureRepo.findNameByOrganization("org1",false);
        assertThat(orgs.size()).isEqualTo(1);
        assertThat(orgs.get(0)).isEqualToIgnoringCase("m1");

    }

    @Test
    public void testFindOrgByName() {
        String org = measureRepo.findOrgByName("m2");
        assertThat(org).isEqualTo("org2");
    }


    public void setEntityManager() throws Exception {
        Measure measure = createATestMeasure("m1", "org1");
        entityManager.persistAndFlush(measure);

        Measure measure2 = createATestMeasure("m2", "org2");
        entityManager.persistAndFlush(measure2);

        Measure measure3 = createATestMeasure("m3", "org3");
        entityManager.persistAndFlush(measure3);
    }
}
