package org.apache.griffin.core.metric;

import org.apache.griffin.core.metric.model.MetricValue;

import java.util.List;

public interface MetricStore {

    List<MetricValue> getMetricValues(String metricName, int size);

    String addMetricValues(List<MetricValue> metricValues);

    String deleteMetricValues(String metricName);
}
