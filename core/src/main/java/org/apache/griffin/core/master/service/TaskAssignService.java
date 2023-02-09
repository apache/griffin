package org.apache.griffin.core.master.service;

import org.apache.griffin.core.master.strategy.AbstractAssignStrategy;
import org.apache.griffin.core.master.strategy.AssignStrategyFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;


//@Component
public class TaskAssignService {

    @Value("${task.assign.strategy}")
    private String assignTaskStrategtClass;

    private AbstractAssignStrategy strategy;

    @PostConstruct
    public void init() {
        strategy = AssignStrategyFactory.getStrategy(assignTaskStrategtClass);
        Assert.notNull(strategy, "Task Assign Strategy init failed");
    }


//    public String assignTask(long instanceId) {
//        return strategy.assignTask(instanceId);
//    }
}
