package org.apache.griffin.core.metric.entity;


import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;

import javax.persistence.Entity;


@Entity
public class MetricTemplate extends AbstractAuditableEntity {
    private static final long serialVersionUID = 7073764585880960522L;

    private String name;
    private String description;
    private String organization;
    private String owner;
    private String creatorType;
    private String creatorId;
    private String metricName;


    public MetricTemplate() {
    }

    public MetricTemplate(String name, String description, String organization, String owner, String creatorType, String creatorId, String metricName) {
        this.name = name;
        this.description = description;
        this.organization = organization;
        this.owner = owner;
        this.creatorType = creatorType;
        this.creatorId = creatorId;
        this.metricName = metricName;
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

    public String getCreatorType() {
        return creatorType;
    }

    public void setCreatorType(String creatorType) {
        this.creatorType = creatorType;
    }

    public String getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(String creatorId) {
        this.creatorId = creatorId;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }
}
