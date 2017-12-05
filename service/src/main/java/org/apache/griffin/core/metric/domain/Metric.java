package org.apache.griffin.core.metric.domain;

import org.apache.griffin.core.metric.entity.MetricTemplate;

import java.util.List;

public class Metric {

    private MetricTemplate metricTemplate;
    private List<MetricValue> metricValues;

    public Metric() {
    }

    public Metric(MetricTemplate metricTemplate, List<MetricValue> metricValues) {
        this.metricTemplate = metricTemplate;
        this.metricValues = metricValues;
    }

    public MetricTemplate getMetricTemplate() {
        return metricTemplate;
    }

    public void setMetricTemplate(MetricTemplate metricTemplate) {
        this.metricTemplate = metricTemplate;
    }

    public List<MetricValue> getMetricValues() {
        return metricValues;
    }

    public void setMetricValues(List<MetricValue> metricValues) {
        this.metricValues = metricValues;
    }
}
