package org.apache.griffin.core.job;

import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.TriggerListener;

import java.util.concurrent.CountDownLatch;

public class CountDownTriggerListener implements TriggerListener {
    private CountDownLatch latch;
    private String name;


    public CountDownTriggerListener(CountDownLatch latch, String name) {
        this.latch = latch;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext context) {
    }

    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
        return false;
    }

    @Override
    public void triggerMisfired(Trigger trigger) {
        latch.countDown();
    }

    @Override
    public void triggerComplete(Trigger trigger, JobExecutionContext context, Trigger.CompletedExecutionInstruction triggerInstructionCode) {
        latch.countDown();
    }
}
