package org.apache.griffin.common.cluster.plan;

public class DatePartitioned implements IPartitionable{
    @Override
    public String getPartitionKey() {
        return "DT";
    }
}
