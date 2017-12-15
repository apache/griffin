package org.apache.griffin.core.job.entity;

import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;

import javax.persistence.*;

@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "type")
public abstract class Job extends AbstractAuditableEntity {

    protected String name;
    protected Long measureId;
    protected String metricName;
    protected Boolean deleted = false;

    public Job() {
    }

    public Job(String name, Long measureId, String metricName) {
        this.name = name;
        this.measureId = measureId;
        this.metricName = metricName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getMeasureId() {
        return measureId;
    }

    public void setMeasureId(Long measureId) {
        this.measureId = measureId;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }
}
