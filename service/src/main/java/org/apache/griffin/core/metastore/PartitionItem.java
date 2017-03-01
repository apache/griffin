package org.apache.griffin.core.metastore;

import java.io.Serializable;

public class PartitionItem implements Serializable {

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    private String name;

    private String value;

    public String generateString() {
        return name + "=" + value;
    }
}
