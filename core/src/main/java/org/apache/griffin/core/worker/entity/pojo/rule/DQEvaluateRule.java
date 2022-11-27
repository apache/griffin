package org.apache.griffin.core.worker.entity.pojo.rule;

import org.apache.griffin.core.worker.entity.pojo.Metric;
import org.apache.griffin.core.worker.utils.ExpressionUtils;

import java.util.List;

public class DQEvaluateRule {

    private String expression;

    public boolean execute(List<Metric> metricList) {
        for (Metric metric : metricList) {
            double metricValue = metric.getMetric();
            if (ExpressionUtils.evaluate(expression, metricValue)) {
                return true;
            }
        }
        return false;
    }
}
