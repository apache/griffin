package org.apache.griffin.core.measure.entity;

import javax.persistence.Entity;

/**
 * Measures to publish metrics that processed externally
 */
@Entity
public class ExternalMeasure extends Measure {

    private String metricName;

    public ExternalMeasure() {
        super();
    }

    public ExternalMeasure(String name, String description, String organization, String owner, String metricName) {
        super(name, description, organization, owner);
        this.metricName = metricName;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    @Override
    public String getType() {
        return "external";
    }
}
