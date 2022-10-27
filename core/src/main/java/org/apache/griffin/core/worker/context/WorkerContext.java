package org.apache.griffin.core.worker.context;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Queue;

/**
 * 上下文信息  全局唯一
 */
@Component
public class WorkerContext {

    public static final Queue<DQInstance> waittingTaskIdQueue = Queues.newPriorityQueue();
//    public static final List<DQBaseTask> runningTaskIdQueue = Lists.newCopyOnWriteArrayList();
    // runningTaskIdList = RECORDING_TASK_LIST + EVALUATING_TASK_LIST + ALERTING_TASK_LIST
    public static final Queue<DQInstance> RECORDING_TASK_QUEUE = Queues.newPriorityQueue();
    public static final Queue<DQInstance> EVALUATING_TASK_QUEUE = Queues.newPriorityQueue();
    public static final Queue<DQInstance> ALERTING_TASK_QUEUE = Queues.newPriorityQueue();

    // success和failed队列数据老化问题？
    public static final List<DQInstance> successTaskIdList = Lists.newArrayList();
    public static final List<DQInstance> failedTaskIdList = Lists.newArrayList();

    /**
     * 启动时，加载让分配在该节点的任务信息到
     */
    @PostConstruct
    public void resetTaskStatusWhenStartUp() {

    }

    // 统计当前节点任务信息
    public void getWorkerTaskStatus() {

    }

}
