package org.apache.griffin.core.metric.repo;

import org.apache.griffin.core.metric.entity.MetricTemplate;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface MetricTemplateRepo extends CrudRepository<MetricTemplate, Long> {

    List<MetricTemplate> findByMetricName(String metricName);

    List<MetricTemplate> findByCreatorTypeAndAndCreatorId(String creatorType, String creatorId);
}
