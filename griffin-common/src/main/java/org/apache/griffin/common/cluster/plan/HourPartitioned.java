package org.apache.griffin.common.cluster.plan;

public class HourPartitioned implements IPartitionable{
    @Override
    public String getPartitionKey() {
        return "HOUR";
    }
}
