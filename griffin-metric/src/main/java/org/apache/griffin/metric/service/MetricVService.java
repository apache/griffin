package org.apache.griffin.metric.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.MetricVDao;
import org.apache.griffin.metric.entity.BaseEntity;
import org.apache.griffin.metric.entity.MetricV;
import org.apache.griffin.metric.exception.GriffinErr;
import org.apache.griffin.metric.exception.GriffinException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class MetricVService {
    public static final String METRIC_V_URI = "/metricV";

    private final MetricVDao metricVDao;

    public MetricVService(MetricVDao metricVDao) {
        this.metricVDao = metricVDao;
    }

    @PutMapping(value = METRIC_V_URI,consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseEntity> createMetricV(@RequestBody MetricV metricV) {
        try {
            int id = metricVDao.addMetricV(metricV);
            if (id != 1) {
                return new ResponseEntity<>(GriffinErr.dbInsertionError.buildErrorEntity(),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return new ResponseEntity<>(metricV, HttpStatus.CREATED);
        } catch (GriffinException e) {
            return new ResponseEntity<>(e.getError().buildErrorEntity(e.getMessage()),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = METRIC_V_URI, consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<BaseEntity> updateMetricV(@RequestBody MetricV metricV) {
        boolean ret = metricVDao.updateById(metricV);
        return ret ? new ResponseEntity<>(metricV, HttpStatus.ACCEPTED) : new ResponseEntity<>(
                GriffinErr.dbUpdateError.buildErrorEntity(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @DeleteMapping(value = METRIC_V_URI + "/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseEntity> deleteMetricV(@PathVariable @NonNull String id) {
        boolean ret = metricVDao.deleteById(id);
        return ret ? new ResponseEntity<>(HttpStatus.OK) : new ResponseEntity<>(
                GriffinErr.dbDeletionError.buildErrorEntity(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
