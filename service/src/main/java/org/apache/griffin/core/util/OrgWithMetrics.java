package org.apache.griffin.core.util;

import java.util.List;

/**
 * Created by xiangrchen on 5/15/17.
 */
public class OrgWithMetrics {
    String org;
    List<String> measureName;

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public List<String> getMeasureName() {
        return measureName;
    }

    public void setMeasureName(List<String> measureName) {
        this.measureName = measureName;
    }
    public OrgWithMetrics(){}


    public OrgWithMetrics(String org, List<String> measureName) {
        this.org = org;
        this.measureName = measureName;
    }
}
