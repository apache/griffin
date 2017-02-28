package org.apache.griffin.core.spec;


/**
 * DataAsset based on hive metadata.
 */

public class HiveDataAsset implements DataAsset {
    public String getName() {
        return name;
    }

    public String getOrg() {
        return org;
    }

    public String getOwner() {
        return owner;
    }

    private String owner;
    private String org;
    private String name;
}
