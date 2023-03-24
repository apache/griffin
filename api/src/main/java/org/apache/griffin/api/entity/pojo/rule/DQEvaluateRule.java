package org.apache.griffin.api.entity.pojo.rule;

import org.apache.griffin.api.entity.pojo.Metric;
import org.apache.griffin.api.utils.ExpressionUtils;

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
