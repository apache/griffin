package org.apache.griffin.core.master.strategy;

import org.apache.griffin.core.api.context.DQCApplicationContext;

public class LooperAssignStrategy extends AbstractAssignStrategy {

    public LooperAssignStrategy(DQCApplicationContext dqcApplicationContext) {
        super(dqcApplicationContext);
    }

    @Override
    public String assignTask(long instanceId) {
        return null;
    }
}
