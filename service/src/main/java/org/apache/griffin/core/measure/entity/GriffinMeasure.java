package org.apache.griffin.core.measure.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.collections.CollectionUtils;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Measures processed on Griffin
 */
@Entity
public class GriffinMeasure extends Measure {

    private String processType;

    @Transient
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long timestamp;


    @NotNull
    @OneToMany(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "measure_id")
    private List<DataSource> dataSources = new ArrayList<>();

    @NotNull
    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "evaluate_rule_id")
    private EvaluateRule evaluateRule;

    public GriffinMeasure() {
        super();
    }

    public GriffinMeasure(String name, String description, String organization, String processType, String owner, List<DataSource> dataSources, EvaluateRule evaluateRule) {
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

    @JsonProperty("data.sources")
    public List<DataSource> getDataSources() {
        return dataSources;
    }

    @JsonProperty("data.sources")
    public void setDataSources(List<DataSource> dataSources) {
        if (CollectionUtils.isEmpty(dataSources)) {
            throw new NullPointerException("Data source can not be empty.");
        }
        this.dataSources = dataSources;
    }

    @JsonProperty("evaluate.rule")
    public EvaluateRule getEvaluateRule() {
        return evaluateRule;
    }

    @JsonProperty("evaluate.rule")
    public void setEvaluateRule(EvaluateRule evaluateRule) {
        if (evaluateRule == null || CollectionUtils.isEmpty(evaluateRule.getRules())) {
            throw new NullPointerException("Evaluate rule can not be empty.");
        }
        this.evaluateRule = evaluateRule;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String getType() {
        return "griffin";
    }
}
