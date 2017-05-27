package org.apache.griffin.core.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/metrics")
public class MetricController {
    private static final Logger log = LoggerFactory.getLogger(MetricController.class);
    @Autowired
    MetricService metricService;
    @RequestMapping("/{measureName}/org")
    public String getOrgByMeasureName(@PathVariable("measureName") String measureName){
        return metricService.getOrgByMeasureName(measureName);
    }



}
