package org.apache.griffin.metric.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.MetricDDao;
import org.apache.griffin.metric.entity.BaseEntity;
import org.apache.griffin.metric.entity.MetricD;
import org.apache.griffin.metric.exception.GriffinErr;
import org.apache.griffin.metric.exception.GriffinException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Slf4j
public class MetricDService {

    public static final String METRICD_URI = "/metricD";

    private final MetricDDao metricDDao;

    public MetricDService(MetricDDao metricDDao) {
        this.metricDDao = metricDDao;
    }

    @GetMapping(value = "/ping", produces = MediaType.APPLICATION_JSON_VALUE)
    public String ping() {
        return "hello";
    }

    @GetMapping(value = "/allMetricD", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<MetricD> allMetricDs() {
        return metricDDao.queryAll();
    }

    @PutMapping(value = METRICD_URI, consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseEntity> createMetricD(@RequestBody MetricD metricD) {
        try {
            int id = metricDDao.addMetricD(metricD);
            if (id != 1) {
                return new ResponseEntity<>(GriffinErr.dbInsertionError.buildErrorEntity(),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return new ResponseEntity<>(metricD, HttpStatus.CREATED);
        } catch (GriffinException e) {
            return new ResponseEntity<>(e.getError().buildErrorEntity(e.getMessage()),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = METRICD_URI, consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<BaseEntity> updateMetricD(MetricD metricD) {
        boolean ret = metricDDao.updateById(metricD);
        return ret ? new ResponseEntity<>(metricD, HttpStatus.ACCEPTED) : new ResponseEntity<>(
                GriffinErr.dbUpdateError.buildErrorEntity(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @DeleteMapping(value = METRICD_URI + "/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseEntity> deleteMetricD(@PathVariable @NonNull String id) {
        boolean ret = metricDDao.deleteById(id);
        return ret ? new ResponseEntity<>(HttpStatus.OK) : new ResponseEntity<>(
                GriffinErr.dbDeletionError.buildErrorEntity(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
