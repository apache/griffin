package org.apache.griffin.core.master.strategy;


import org.apache.griffin.api.context.DQCApplicationContext;

public abstract class AbstractAssignStrategy {

    protected DQCApplicationContext dqcApplicationContext;

    public AbstractAssignStrategy(DQCApplicationContext dqcApplicationContext) {
        this.dqcApplicationContext = dqcApplicationContext;
    }

    public abstract String assignTask(long instanceId);
}
