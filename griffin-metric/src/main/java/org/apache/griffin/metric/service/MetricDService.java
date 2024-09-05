package org.apache.griffin.metric.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.MetricDDao;
import org.apache.griffin.metric.entity.MetricD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Slf4j
public class MetricDService {
    @GetMapping(value = "/ping", produces = MediaType.APPLICATION_JSON_VALUE)
    public String ping(){
        return "hello";
    }

    @Autowired
    private MetricDDao metricDDao;

    @GetMapping(value = "/allMetricDs", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<MetricD> allMetricDs(){
        return metricDDao.queryAll();
    }
}
