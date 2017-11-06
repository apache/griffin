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

package org.apache.griffin.core.measure;


import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class MeasureOrgServiceImplTest {

    @InjectMocks
    private MeasureOrgServiceImpl service;

    @Mock
    private MeasureRepo measureRepo;

    @Test
    public void testGetOrgs(){
        String orgName = "orgName";
        given(measureRepo.findOrganizations(false)).willReturn(Arrays.asList(orgName));
        List<String> orgs =service.getOrgs();
        assertThat(orgs.size()).isEqualTo(1);
        assertThat(orgs.get(0)).isEqualTo(orgName);
    }

    @Test
    public void testGetMetricNameListByOrg(){
        String orgName = "orgName";
        String measureName = "measureName";
        given(measureRepo.findNameByOrganization(orgName,false)).willReturn(Arrays.asList(measureName));
        List<String> measureNames=service.getMetricNameListByOrg(orgName);
        assertThat(measureNames.size()).isEqualTo(1);
        assertThat(measureNames.get(0)).isEqualTo(measureName);
    }

    @Test
    public void testGetMeasureNamesGroupByOrg(){
        Measure measure = new Measure("measure", "desc", "org", "proctype", "owner", null, null);
        List<Measure> measures = new ArrayList<>();
        measures.add(measure);

        when(measureRepo.findByDeleted(false)).thenReturn(measures);

        Map<String,List<String>> map = service.getMeasureNamesGroupByOrg();
        assertThat(map.size()).isEqualTo(1);

    }

}