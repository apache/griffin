package org.apache.griffin.core.metric;

import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by xiangrchen on 5/8/17.
 */
@RestController
@RequestMapping("/metrics")
public class MetricController {


//    @RequestMapping("/organization/{org}/{measureName}")
private static final Logger log = LoggerFactory.getLogger(MetricController.class);
    @Autowired
    MeasureRepo measureRepo;
    @RequestMapping("/org/{measureName}")
    public String getOrgByMeasureName(@PathVariable("measureName") String measureName){
        return measureRepo.findOrgByName(measureName);
    }

}
