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

import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MeasureOrgServiceImpl implements MeasureOrgService {

    @Autowired
    private MeasureRepo measureRepo;

    @Override
    public List<String> getOrgs() {
        return measureRepo.findOrganizations();
    }

    @Override
    public List<String> getMetricNameListByOrg(String org) {
        return measureRepo.findNameByOrganization(org);
    }

    @Override
    public Map<String, List<String>> getMeasureNamesGroupByOrg() {
        Map<String, List<String>> orgWithMetricsMap = new HashMap<>();
        List<String> orgList = measureRepo.findOrganizations();
        for (String org : orgList) {
            if (org != null) {
                orgWithMetricsMap.put(org, measureRepo.findNameByOrganization(org));
            }
        }
        return orgWithMetricsMap;
    }
}
