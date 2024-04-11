package org.apache.griffin.common.cluster.plan;

public interface ITriggerStrategy {
    IPartitionable yield(String param);
}
