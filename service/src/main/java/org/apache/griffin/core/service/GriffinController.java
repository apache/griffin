package org.apache.griffin.core.service;


import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.OrgWithMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;


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
    public List<OrgWithMetrics> getOrgsWithMetrics(){
        List<OrgWithMetrics> orgWithMetricsList=new ArrayList<OrgWithMetrics>();
        List<String> orgList=measureRepo.findOrganizations();
        for (String org:orgList){
            OrgWithMetrics orgWithMetrics=new OrgWithMetrics();
            orgWithMetrics.setOrg(org);
            orgWithMetrics.setMeasureName(measureRepo.findNameByOrganization(org));
            orgWithMetricsList.add(orgWithMetrics);
        }
        return orgWithMetricsList;

    }
}

