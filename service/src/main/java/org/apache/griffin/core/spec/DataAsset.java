package org.apache.griffin.core.spec;


import java.io.Serializable;

public interface DataAsset extends Serializable {
    public String getName();
    public String getOrg();
    public String getOwner();
}
