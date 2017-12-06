package org.apache.griffin.core.metric;

import org.apache.griffin.core.metric.domain.MetricValue;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;

@Component
public class MetricStoreImpl implements MetricStore {

    @Override
    public List<MetricValue> getMetricValues(String metricName) {
        return null;
    }

    @Override
    public GriffinOperationMessage addMetricValues(List<MetricValue> metricValues) {
        return null;
    }

    @Override
    public GriffinOperationMessage deleteMetricValues(String metricName) {
        return null;
    }
}
