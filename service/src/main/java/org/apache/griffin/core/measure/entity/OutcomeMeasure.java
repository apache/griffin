package org.apache.griffin.core.measure.entity;

import javax.persistence.Entity;

@Entity
public class OutcomeMeasure extends Measure {

    private String metricName;

    public OutcomeMeasure() {
        super();
    }

    public OutcomeMeasure(String name, String description, String organization, String owner, String metricName) {
        super(name, description, organization, owner);
        this.metricName = metricName;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }
}
