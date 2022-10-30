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

    private final Queue<DQInstance> WAITTING_TASK_QUEUE;
//    public static final List<DQBaseTask> runningTaskIdQueue = Lists.newCopyOnWriteArrayList();
    // runningTaskIdList = RECORDING_TASK_LIST + EVALUATING_TASK_LIST + ALERTING_TASK_LIST
    private final Queue<DQInstance> RECORDING_TASK_QUEUE;
    private final Queue<DQInstance> EVALUATING_TASK_QUEUE;
    private final Queue<DQInstance> ALERTING_TASK_QUEUE;

    // success和failed队列数据老化问题？
    public final List<DQInstance> successTaskIdList;
    public final List<DQInstance> failedTaskIdList;
    
    public WorkerContext() {
        // 设置队列长度
        WAITTING_TASK_QUEUE = Queues.newPriorityQueue();
        RECORDING_TASK_QUEUE = Queues.newPriorityQueue();
        EVALUATING_TASK_QUEUE = Queues.newPriorityQueue();
        ALERTING_TASK_QUEUE = Queues.newPriorityQueue();
        successTaskIdList = Lists.newArrayList();
        failedTaskIdList = Lists.newArrayList();
    }
    
    @PostConstruct
    public void init() {
        initQueueInfo();
        resetTaskStatusWhenStartUp();
    }

    private void initQueueInfo() {
        
    }

    public DQInstance getWaittingTask() {
        return null;
    }

    public DQInstance getRecordingTask() {
        return null;
    }

    public void offerToRecordingTaskQueue(DQInstance dqInstance) {
    }
    public void offerToAlertingTaskQueue(DQInstance dqInstance) {
    }

    /**
     * 启动时，加载让分配在该节点的任务信息到
     */
    public void resetTaskStatusWhenStartUp() {

    }

    // 统计当前节点任务信息
    public void getWorkerTaskStatus() {

    }

    public boolean canSubmitToDispatcher() {
        return false;
    }


    public void offerToEvaluatingTaskQueue(DQInstance dqInstance) {
    }
}
