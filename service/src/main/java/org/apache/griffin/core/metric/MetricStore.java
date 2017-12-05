package org.apache.griffin.core.metric;

import org.apache.griffin.core.metric.domain.MetricValue;
import org.apache.griffin.core.util.GriffinOperationMessage;

import java.util.List;

public interface MetricStore {

    List<MetricValue> getMetricValues(String metricName);

    GriffinOperationMessage addMetricValues(List<MetricValue> metricValues);

    GriffinOperationMessage deleteMetricValues(String metricName);
}
