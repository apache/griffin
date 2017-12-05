package org.apache.griffin.core.metric.domain;

import java.util.Map;

public class MetricValue {

    private String name;

    private Long tmst;

    private Map<String, Object> value;

    public MetricValue() {
    }

    public MetricValue(String name, Long tmst, Map<String, Object> value) {
        this.name = name;
        this.tmst = tmst;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTmst() {
        return tmst;
    }

    public void setTmst(Long tmst) {
        this.tmst = tmst;
    }

    public Map<String, Object> getValue() {
        return value;
    }

    public void setValue(Map<String, Object> value) {
        this.value = value;
    }
}
