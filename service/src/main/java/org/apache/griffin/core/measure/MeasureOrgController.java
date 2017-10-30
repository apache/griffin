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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(tags = "Organization Dimension", description = "measure belongs to")
@RestController
@RequestMapping(value = "/api/v1")
public class MeasureOrgController {
    @Autowired
    private MeasureRepo measureRepo;

    @ApiOperation(value = "Get orgs for measure", response = List.class)
    @RequestMapping(value = "/org", method = RequestMethod.GET)
    public List<String> getOrgs() {
        return measureRepo.findOrganizations();
    }

    /**
     * @param org
     * @return list of metric name, and a metric is the result of executing the job sharing the same name with
     * measure.
     */
    @ApiOperation(value = "Get measure names by org", response = List.class)
    @RequestMapping(value = "/org/{org}", method = RequestMethod.GET)
    public List<String> getMetricNameListByOrg(@ApiParam(value = "organization name") @PathVariable("org") String org) {
        return measureRepo.findNameByOrganization(org);
    }

    @ApiOperation(value = "Get measure names group by org", response = Map.class)
    @RequestMapping(value = "/org/measure/names", method = RequestMethod.GET)
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
