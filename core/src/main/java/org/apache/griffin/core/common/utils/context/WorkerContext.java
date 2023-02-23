package org.apache.griffin.core.common.utils.context;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.griffin.core.worker.entity.bo.DQInstanceBO;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Worker Runtime Env
 * Scope: Singleton
 */
@Component
public class WorkerContext {

    private final List<DQInstanceBO> WAITTING_TASK_QUEUE;
    private final List<DQInstanceBO> RECORDING_TASK_QUEUE;
    private final LinkedBlockingQueue<DQInstanceBO> EVALUATING_TASK_QUEUE;
    private final LinkedBlockingQueue<DQInstanceBO> ALERTING_TASK_QUEUE;

    // success和failed队列数据老化问题？
    public final List<DQInstanceBO> successTaskIdList;
    public final List<DQInstanceBO> failedTaskIdList;
    
    public WorkerContext() {
        // 设置队列长度
        WAITTING_TASK_QUEUE = Lists.newCopyOnWriteArrayList();
        RECORDING_TASK_QUEUE = Lists.newCopyOnWriteArrayList();
        // 这两个应该是一个阻塞队列 只要有任务来就可以处理
        EVALUATING_TASK_QUEUE = Queues.newLinkedBlockingQueue();
        ALERTING_TASK_QUEUE =Queues.newLinkedBlockingQueue();
        successTaskIdList = Lists.newArrayList();
        failedTaskIdList = Lists.newArrayList();
    }

    public List<DQInstanceBO> getWAITTING_TASK_QUEUE() {
        return WAITTING_TASK_QUEUE;
    }

    public List<DQInstanceBO> getRECORDING_TASK_QUEUE() {
        return RECORDING_TASK_QUEUE;
    }

    public LinkedBlockingQueue<DQInstanceBO> getEVALUATING_TASK_QUEUE() {
        return EVALUATING_TASK_QUEUE;
    }

    public LinkedBlockingQueue<DQInstanceBO> getALERTING_TASK_QUEUE() {
        return ALERTING_TASK_QUEUE;
    }

    public List<DQInstanceBO> getSuccessTaskIdList() {
        return successTaskIdList;
    }

    public List<DQInstanceBO> getFailedTaskIdList() {
        return failedTaskIdList;
    }

    @PostConstruct
    public void init() {
        resetTaskStatusWhenStartUp();
    }

    public DQInstanceBO getWaittingTask() {
        return null;
    }

    public DQInstanceBO getRecordingTask() {
        return null;
    }

    public boolean offerToRecordingTaskQueue(DQInstanceBO dqInstance) {
        return false;
    }
    public void offerToAlertingTaskQueue(DQInstanceBO dqInstance) {
    }

    /**
     * 启动时，加载让分配在该节点的任务信息到
     */
    public void resetTaskStatusWhenStartUp() {

    }

    // 统计当前节点任务信息
    public void getWorkerTaskStatus() {

    }

    public boolean canSubmitToSpecEngine(DQEngineEnum engine) {
        return false;
    }


    public void offerToEvaluatingTaskQueue(DQInstanceBO dqInstance) {
    }

    public void removeAll(List<DQInstanceBO> targetList, List<DQInstanceBO> waittingToRemoveFromRecordingList) {
        targetList.removeAll(waittingToRemoveFromRecordingList);
    }

    public void addFailedDQInstanceInfo(DQInstanceBO instance) {

    }

    public void addSuccessDQInstanceInfo(DQInstanceBO instance) {

    }
}
