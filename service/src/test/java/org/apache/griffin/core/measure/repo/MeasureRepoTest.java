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

import org.apache.griffin.core.measure.entity.DataConnector;
import org.apache.griffin.core.measure.entity.EvaluateRule;
import org.apache.griffin.core.measure.entity.Measure;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.List;

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
    public void testFindAllOrganizations() throws Exception {
        List<String> orgs = measureRepo.findOrganizations();
        assertThat(orgs.size()).isEqualTo(3);
    }


    @Test
    public void testFindNameByOrganization() throws Exception {
        List<String> orgs = measureRepo.findNameByOrganization("org1");
        assertThat(orgs.size()).isEqualTo(1);
        assertThat(orgs.get(0)).isEqualToIgnoringCase("m2");

    }

    @Test
    public void testFindOrgByName() throws Exception {
        String org = measureRepo.findOrgByName("m3");
        assertThat(org).isEqualTo("org2");
    }

    private Measure createATestMeasure(String name,String org)throws Exception{
        HashMap<String,String> configMap1=new HashMap<>();
        configMap1.put("database","default");
        configMap1.put("table.name","test_data_src");
        HashMap<String,String> configMap2=new HashMap<>();
        configMap2.put("database","default");
        configMap2.put("table.name","test_data_tgt");
        String configJson1 = new org.codehaus.jackson.map.ObjectMapper().writeValueAsString(configMap1);
        String configJson2 = new org.codehaus.jackson.map.ObjectMapper().writeValueAsString(configMap2);

        DataConnector source = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);

        String rules = "$source.uage > 100 AND $source.uid = $target.uid AND $source.uage + 12 = $target.uage + 10 + 2 AND $source.udes + 11 = $target.udes + 1 + 1";

        EvaluateRule eRule = new EvaluateRule(1,rules);

        Measure measure = new Measure(name,"bevssoj description", Measure.MearuseType.accuracy, org, source, target, eRule,"test1");

        return measure;
    }

    public void setEntityManager() throws Exception {
        Measure measure=createATestMeasure("m1","bullseye");
        entityManager.persistAndFlush(measure);

        Measure measure2=createATestMeasure("m2","org1");
        entityManager.persistAndFlush(measure2);

        Measure measure3=createATestMeasure("m3","org2");
        entityManager.persistAndFlush(measure3);
    }
}
