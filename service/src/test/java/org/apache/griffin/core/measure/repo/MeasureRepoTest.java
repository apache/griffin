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

import org.apache.griffin.core.config.EclipseLinkJpaConfigForTest;
import org.apache.griffin.core.measure.entity.Measure;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.apache.griffin.core.util.EntityHelper.createGriffinMeasure;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@DataJpaTest
@ContextConfiguration(classes = {EclipseLinkJpaConfigForTest.class})
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
    public void testFindByNameAndDeleted() {
        String name = "m1";
        List<Measure> measures = measureRepo.findByNameAndDeleted(name, false);
        assertThat(measures.get(0).getName()).isEqualTo(name);
    }

    @Test
    public void testFindByDeleted() {
        List<Measure> measures = measureRepo.findByDeleted(false);
        assertThat(measures.size()).isEqualTo(3);
    }

    @Test
    public void testFindByOwnerAndDeleted() {
        List<Measure> measures = measureRepo.findByOwnerAndDeleted("test", false);
        assertThat(measures.size()).isEqualTo(2);
    }

    @Test
    public void testFindByIdAndDeleted() {
        Measure measure = measureRepo.findByIdAndDeleted(1L, true);
        assertThat(measure).isNull();
    }

    @Test
    public void testFindOrganizations() {
        List<String> organizations = measureRepo.findOrganizations(false);
        assertThat(organizations.size()).isEqualTo(3);
    }

    @Test
    public void testFindNameByOrganization() {
        List<String> names = measureRepo.findNameByOrganization("org1", false);
        assertThat(names.size()).isEqualTo(1);
    }

    public void setEntityManager() throws Exception {
        Measure measure1 = createGriffinMeasure("m1");
        measure1.setOrganization("org1");
        entityManager.persistAndFlush(measure1);

        Measure measure2 = createGriffinMeasure("m2");
        measure2.setOrganization("org2");
        entityManager.persistAndFlush(measure2);

        Measure measure3 = createGriffinMeasure("m3");
        measure3.setOrganization("org3");
        measure3.setOwner("owner");
        entityManager.persistAndFlush(measure3);
    }
}
