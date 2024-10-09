package org.apache.griffin.metric.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.MetricDDao;
import org.apache.griffin.metric.entity.MetricD;
import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Slf4j
public class MetricDService {

    private final MetricDDao metricDDao;

    public MetricDService(MetricDDao metricDDao) {
        this.metricDDao = metricDDao;
    }

    @GetMapping(value = "/ping", produces = MediaType.APPLICATION_JSON_VALUE)
    public String ping(){
        return "hello";
    }

    @GetMapping(value = "/allMetricDs", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<MetricD> allMetricDs(){
        return metricDDao.queryAll();
    }

    @PutMapping(value = "/metricD", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public MetricD createMetricD(@RequestBody MetricD metricD){
        int id = metricDDao.addMetricD(metricD);
        return metricD;
    }

    @PostMapping(value = "/metricD", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public MetricD updateMetricD(MetricD metricD){
        boolean ret = metricDDao.updateById(metricD);
        return ret ? metricD : null;
    }

    @DeleteMapping(value = "/metricD/{id}")
    public boolean deleteMetricD(@PathVariable @NonNull String id){
        return metricDDao.deleteById(id);
    }
}
