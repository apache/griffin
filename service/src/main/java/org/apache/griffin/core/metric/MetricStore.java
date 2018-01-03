package org.apache.griffin.core.metric;

import org.apache.griffin.core.metric.model.MetricValue;

import java.util.List;

public interface MetricStore {

    List<MetricValue> getMetricValues(String metricName, int from, int size) throws Exception;

    void addMetricValue(MetricValue metricValue) throws Exception;

    void deleteMetricValues(String metricName) throws Exception;
}
