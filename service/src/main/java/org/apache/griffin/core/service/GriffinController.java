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

package org.apache.griffin.core.service;


import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
public class GriffinController {
    private static final Logger LOGGER = LoggerFactory.getLogger(GriffinController.class);

    @Autowired
    MeasureRepo measureRepo;

    @RequestMapping(value = "/version", method = RequestMethod.GET)
    public String greeting() {
        return "0.1.0";
    }

    @RequestMapping(value = "/org", method = RequestMethod.GET)
    public List<String> getOrgs() {
        return measureRepo.findOrganizations();
    }

    /**
     * @param org
     * @return list of the name of metric, and a metric is the result of executing the job sharing the same name with
     * measure.
     */
    @RequestMapping(value = "/org/{org}", method = RequestMethod.GET)
    public List<String> getMetricNameListByOrg(@PathVariable("org") String org) {
        return measureRepo.findNameByOrganization(org);
    }

    @RequestMapping(value = "/orgWithMetricsName", method = RequestMethod.GET)
    public Map<String, List<String>> getOrgsWithMetricsName() {
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

