package org.apache.griffin.core.metric;

import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.entity.OutcomeMeasure;
import org.apache.griffin.core.metric.entity.MetricTemplate;

import java.util.List;

public interface  MetricTemplateService {

    List<MetricTemplate> getAllTemplates();

    MetricTemplate getTemplateById(Long id);

    MetricTemplate getTemplateByMetricName(String metricName);

    void createTemplateFromMeasure(OutcomeMeasure measure);

    void updateTemplateFromMeasure(OutcomeMeasure measure);

    void deleteTemplateFromMeasure(OutcomeMeasure measure);

    void createTemplateFromJob(Measure measure, String jobId, String jobName);

    void deleteTemplateFromJob(String jobId, String jobName);
}
