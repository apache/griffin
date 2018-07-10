package org.apache.griffin.core.job.entity;

public enum JobType {

    BATCH("batch"), //
    STREAMING("streaming"), //
    VIRTUAL("virtual");

    private String name;

    private JobType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
