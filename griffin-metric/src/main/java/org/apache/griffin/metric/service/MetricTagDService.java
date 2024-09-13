package org.apache.griffin.metric.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.MetricTagDDao;
import org.apache.griffin.metric.dao.TagAttachmentDao;
import org.apache.griffin.metric.entity.MetricTagD;
import org.apache.griffin.metric.entity.TagAttachment;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class MetricTagDService {
    private final MetricTagDDao metricTagDDao;

    private final TagAttachmentDao tagAttachmentDao;

    public MetricTagDService(MetricTagDDao metricTagDDao, TagAttachmentDao tagAttachmentDao) {
        this.metricTagDDao = metricTagDDao;
        this.tagAttachmentDao = tagAttachmentDao;
    }

    @PutMapping(value = "/metricTagD",consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public MetricTagD createMetricTagD(@RequestBody MetricTagD metricTagD) {
        int i = metricTagDDao.addMetricTagD(metricTagD);
        return metricTagD;
    }

    @PutMapping(value = "/tags",consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public TagAttachment attachTags(@RequestBody TagAttachment tagAttachment) {
        int i = tagAttachmentDao.addMetricTags(tagAttachment);
        return tagAttachment;
    }
}
