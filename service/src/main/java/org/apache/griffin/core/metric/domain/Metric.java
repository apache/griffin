package org.apache.griffin.core.metric.domain;

import org.apache.griffin.core.metric.entity.MetricTemplate;

import java.util.List;

public class Metric {

    private String name;
    private String description;
    private String organization;
    private String owner;
    private List<MetricValue> metricValues;

    public Metric() {
    }

    public Metric(String name, String description, String organization, String owner, List<MetricValue> metricValues) {
        this.name = name;
        this.description = description;
        this.organization = organization;
        this.owner = owner;
        this.metricValues = metricValues;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public List<MetricValue> getMetricValues() {
        return metricValues;
    }

    public void setMetricValues(List<MetricValue> metricValues) {
        this.metricValues = metricValues;
    }
}
