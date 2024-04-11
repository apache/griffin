package org.apache.griffin.common.cluster;

import org.apache.griffin.common.cluster.plan.ITriggerStrategy;
import org.apache.griffin.common.model.IJob;
import org.apache.griffin.common.model.IJobInstance;
import org.apache.griffin.common.model.ITask;

import java.util.List;

public interface IScheduler {
    /**
     * TODO list is not good for DAG jobs.
     * @param job
     * @return
     */
    List<IJobInstance> trigger(IJob job, ITriggerStrategy triggerStrategy);

    List<ITask> plan(IJobInstance jobInstance);

    TaskScheduleStatus schedule(ITask task);


}
