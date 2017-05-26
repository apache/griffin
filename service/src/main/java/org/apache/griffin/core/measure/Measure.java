package org.apache.griffin.core.measure;

import javax.persistence.*;


@Entity
public class Measure extends AuditableEntity   {

    private static final long serialVersionUID = -4748881017029815794L;

    @Column(unique=true)
    private String name;

    private String description;

    public static enum MearuseType {
        accuracy,
    }

    private String organization;
    @Enumerated(EnumType.STRING)
    private MearuseType type;

    @ManyToOne(fetch = FetchType.EAGER,cascade = {CascadeType.PERSIST, CascadeType.REMOVE})
    @JoinColumn(name = "source_id")
    private DataConnector source;

    @ManyToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE})
    @JoinColumn(name = "target_id")
    private DataConnector target;

    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE})
    @JoinColumn(name = "evaluateRule_id")
    private EvaluateRule evaluateRule;

    private String owner;

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

    public MearuseType getType() {
        return type;
    }

    public void setType(MearuseType type) {
        this.type = type;
    }

    public DataConnector getSource() {
        return source;
    }

    public void setSource(DataConnector source) {
        this.source = source;
    }

    public DataConnector getTarget() {
        return target;
    }

    public void setTarget(DataConnector target) {
        this.target = target;
    }

    public EvaluateRule getEvaluateRule() {
        return evaluateRule;
    }

    public void setEvaluateRule(EvaluateRule evaluateRule) {
        this.evaluateRule = evaluateRule;
    }

    public Measure() {
    }

    public Measure(String name, String description, MearuseType type, String organization, DataConnector source, DataConnector target, EvaluateRule evaluateRule, String owner) {
        this.name = name;
        this.description=description;
        this.organization = organization;
        this.type = type;
        this.source = source;
        this.target = target;
        this.evaluateRule = evaluateRule;
        this.owner = owner;
    }


}
