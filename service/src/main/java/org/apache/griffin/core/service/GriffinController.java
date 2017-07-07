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


import org.apache.griffin.core.measure.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;


@RestController
public class GriffinController {

    @RequestMapping("/version")
    public String greeting() {
        return "0.1.0";
    }

    private static final Logger log = LoggerFactory.getLogger(GriffinController.class);
    @Autowired
    MeasureRepo measureRepo;

    @RequestMapping("/org")
    public List<String> getOrgs(){
        return measureRepo.findOrganizations();
    }

    @RequestMapping("/org/{org}")
    public List<String> getMetricNameListByOrg(@PathVariable("org") String org){
        return measureRepo.findNameByOrganization(org);
    }

    @RequestMapping("/orgWithMetrics")
    public Map<String,List<String>> getOrgsWithMetrics(){
        Map<String,List<String>> orgWithMetricsMap=new HashMap<>();
        List<String> orgList=measureRepo.findOrganizations();
        for (String org:orgList){
            if(org!=null){
                orgWithMetricsMap.put(org,measureRepo.findNameByOrganization(org));
            }
        }
        return orgWithMetricsMap;
    }

    @RequestMapping("/dataAssetsWithMetrics")
    public Map<String,List<String>> getDataAssetsWithMetrics(){
        Map<String,List<String>> daWithMetricsMap=new HashMap<>();
        Iterable<Measure> measureList=measureRepo.findAll();
        for (Measure m:measureList){
            switch (m.getType()){
                case accuracy:
                    String[] tableNames={m.getSource().getConfigInMaps().get("table.name"),m.getTarget().getConfigInMaps().get("table.name")};
                    for (String taName:tableNames){
                        if(taName!=null) {
                            if(daWithMetricsMap.get(taName)==null){
                                daWithMetricsMap.put(taName, new ArrayList<>(Arrays.asList(m.getName())));
                            }else{
                                List<String> measureNameList=daWithMetricsMap.get(taName);
                                measureNameList.add(m.getName());
                                daWithMetricsMap.put(taName, measureNameList);
                            }
                        }
                    }
                    break;
                default:
                    log.info("invalid measure type!");
            }

        }
        return daWithMetricsMap;
    }

}

