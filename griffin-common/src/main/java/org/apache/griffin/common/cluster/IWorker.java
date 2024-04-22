package org.apache.griffin.common.cluster;

import org.springframework.context.SmartLifecycle;

public interface IWorker extends SmartLifecycle {
    boolean submitTask(ExecuteTaskParam param);
    void stopTask(long taskid);
}
