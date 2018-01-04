package org.apache.griffin.core.measure.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.griffin.core.job.entity.VirtualJob;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToOne;

/**
 * Measures to publish metrics that processed externally
 */
@Entity
public class ExternalMeasure extends Measure {

    private String metricName;

    @JsonIgnore
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private VirtualJob virtualJob;

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

    public VirtualJob getVirtualJob() {
        return virtualJob;
    }

    public void setVirtualJob(VirtualJob virtualJob) {
        this.virtualJob = virtualJob;
    }

    @Override
    public String getType() {
        return "external";
    }
}
