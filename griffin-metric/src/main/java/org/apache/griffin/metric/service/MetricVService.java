package org.apache.griffin.metric.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.MetricVDao;
import org.apache.griffin.metric.entity.MetricV;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class MetricVService {
    private final MetricVDao metricVDao;

    public MetricVService(MetricVDao metricVDao) {
        this.metricVDao = metricVDao;
    }

    @PutMapping(value = "/metricV",consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public MetricV createMetricV(@RequestBody MetricV metricV) {
        int id = metricVDao.addMetricV(metricV);
        return metricV;
    }
}
