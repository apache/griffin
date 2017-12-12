package org.apache.griffin.core.measure.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.persistence.*;
import java.util.List;

/**
 * Measures processed on Griffin
 */
@Entity
public class GriffinMeasure extends Measure {

    private String processType;

    /**
     * record triggered time of measure
     */
    private Long triggerTimeStamp = -1L;


    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "measure_id")
    private List<DataSource> dataSources;

    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "evaluateRule_id")
    private EvaluateRule evaluateRule;

    public GriffinMeasure() {
        super();
    }

    public GriffinMeasure(String name, String description, String organization, String owner, String processType, List<DataSource> dataSources, EvaluateRule evaluateRule) {
        super(name, description, organization, owner);
        this.processType = processType;
        this.dataSources = dataSources;
        this.evaluateRule = evaluateRule;
    }

    @JsonProperty("process.type")
    public String getProcessType() {
        return processType;
    }

    @JsonProperty("process.type")
    public void setProcessType(String processType) {
        this.processType = processType;
    }

    @JsonProperty("timestamp")
    public Long getTriggerTimeStamp() {
        return triggerTimeStamp;
    }

    @JsonProperty("timestamp")
    public void setTriggerTimeStamp(Long triggerTimeStamp) {
        this.triggerTimeStamp = triggerTimeStamp;
    }

    @JsonProperty("data.sources")
    public List<DataSource> getDataSources() {
        return dataSources;
    }

    @JsonProperty("data.sources")
    public void setDataSources(List<DataSource> dataSources) {
        this.dataSources = dataSources;
    }

    public EvaluateRule getEvaluateRule() {
        return evaluateRule;
    }

    public void setEvaluateRule(EvaluateRule evaluateRule) {
        this.evaluateRule = evaluateRule;
    }

    @Override
    public String getType() {
        return "griffin";
    }
}
