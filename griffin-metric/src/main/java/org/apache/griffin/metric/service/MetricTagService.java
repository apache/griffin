package org.apache.griffin.metric.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.MetricTagDDao;
import org.apache.griffin.metric.dao.TagAttachmentDao;
import org.apache.griffin.metric.entity.BaseEntity;
import org.apache.griffin.metric.entity.MetricTagD;
import org.apache.griffin.metric.entity.TagAttachment;
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
public class MetricTagService {

    public static final String METRIC_TAG_D = "/metricTagD";

    public static final String TAGS = "/tags";

    private final MetricTagDDao metricTagDDao;

    private final TagAttachmentDao tagAttachmentDao;

    public MetricTagService(MetricTagDDao metricTagDDao, TagAttachmentDao tagAttachmentDao) {
        this.metricTagDDao = metricTagDDao;
        this.tagAttachmentDao = tagAttachmentDao;
    }

    @PutMapping(value = METRIC_TAG_D,consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseEntity> createMetricTagD(@RequestBody MetricTagD metricTagD) {
        try {
            int id = metricTagDDao.addMetricTagD(metricTagD);
            if (id != 1) {
                return new ResponseEntity<>(GriffinErr.dbInsertionError.buildErrorEntity(),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return new ResponseEntity<>(metricTagD, HttpStatus.CREATED);
        } catch (GriffinException e) {
            return new ResponseEntity<>(e.getError().buildErrorEntity(e.getMessage()),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = METRIC_TAG_D, consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<BaseEntity> updateMetricTagD(@RequestBody MetricTagD metricTagD) {
        boolean ret = metricTagDDao.updateById(metricTagD);
        return ret ? new ResponseEntity<>(metricTagD, HttpStatus.ACCEPTED) : new ResponseEntity<>(
                GriffinErr.dbUpdateError.buildErrorEntity(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @DeleteMapping(value = METRIC_TAG_D + "/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseEntity> deleteMetricTagD(@PathVariable @NonNull String id) {
        boolean ret = metricTagDDao.deleteById(id);
        return ret ? new ResponseEntity<>(HttpStatus.OK) : new ResponseEntity<>(
                GriffinErr.dbDeletionError.buildErrorEntity(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @PutMapping(value = TAGS,consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseEntity> attachTags(@RequestBody TagAttachment tagAttachment) {
        try {
            int id = tagAttachmentDao.addMetricTags(tagAttachment);
            if (id != 1) {
                return new ResponseEntity<>(GriffinErr.dbInsertionError.buildErrorEntity(),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return new ResponseEntity<>(tagAttachment, HttpStatus.CREATED);
        } catch (GriffinException e) {
            return new ResponseEntity<>(e.getError().buildErrorEntity(e.getMessage()),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = TAGS, consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<BaseEntity> updateMetricD(@RequestBody TagAttachment tagAttachment) {
        boolean ret = tagAttachmentDao.updateById(tagAttachment);
        return ret ? new ResponseEntity<>(tagAttachment, HttpStatus.ACCEPTED) : new ResponseEntity<>(
                GriffinErr.dbUpdateError.buildErrorEntity(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @DeleteMapping(value = TAGS + "/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseEntity> deleteMetricD(@PathVariable @NonNull String id) {
        boolean ret = tagAttachmentDao.deleteById(id);
        return ret ? new ResponseEntity<>(HttpStatus.OK) : new ResponseEntity<>(
                GriffinErr.dbDeletionError.buildErrorEntity(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
