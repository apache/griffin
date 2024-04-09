package org.apache.griffin.core.worker.schedule;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.griffin.core.worker.context.WorkerContext;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQInstanceStatus;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.apache.griffin.core.worker.service.DQInstanceService;
import org.apache.griffin.core.worker.service.DQTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 任务执行调度期 和 dispatcher交互
 */
@Component
public class TaskDispatcherScheduler {
    private static final Logger log = LoggerFactory.getLogger(TaskDispatcherScheduler.class);

    private WorkerContext wc;
    private DQInstanceService dqInstanceService;
    private DQTaskService dqTaskService;

    @Autowired
    public void setWc(WorkerContext wc) {
        this.wc = wc;
    }
    @Autowired
    public void setDqInstanceService(DQInstanceService dqInstanceService) {
        this.dqInstanceService = dqInstanceService;
    }
    @Autowired
    public void setDqTaskService(DQTaskService dqTaskService) {
        this.dqTaskService = dqTaskService;
    }

    @PostConstruct
    public void startEvaluetingAndAlertThread() {
        // todo 用线程池运行
        scanAlertingTask();
        scanEvaluatingTask();
    }

    /**
     * 进行任务调度
     */
    @Scheduled(fixedDelay = 5 * 1000L)
    public void doTaskDispatcherScheduler() {
        log.info("doTaskDispatcherScheduler start.");
        List<DQInstance> waittingToRemoveFromWaitingList = Lists.newArrayList();
        Queue<DQInstance> waitingToRecordingDQInstanceQueue = Queues.newPriorityBlockingQueue(wc.getWAITTING_TASK_QUEUE());
        // 从waitting队列获取任务
        try {
            while (true) {
                try {
                    DQInstance dqInstance = waitingToRecordingDQInstanceQueue.poll();
                    // 队列中无元素 结束循环
                    if (dqInstance == null) break;
                    if (DQInstanceStatus.ACCEPTED != dqInstance.getStatus()) {
                        // 非初始状态
                        // 同步数据库状态 缓存中的实例状态可能不是最新的 从数据库构建最新的实例 替换缓存中的实例
                        dqInstance = dqInstanceService.getById(dqInstance.getId());
                        // 根据状态放到对应队列
                        waittingToRemoveFromWaitingList.add(dqInstance);
                    } else {
                        // 提交到recording队列
                        // 开始提交任务 放到recording队列中
                        if (dqInstanceService.updateStatus(dqInstance, DQInstanceStatus.WAITTING)) {
                            waittingToRemoveFromWaitingList.add(dqInstance);
                        }
                    }
                } catch (Exception e) {
                    // todo
                    log.error("doTaskDispatcherScheduler scan waitting task failed, ex:", e);
                }
            }
        } catch (Exception e) {
            // todo
            log.error("scanRecordingTask failed, ex:", e);
        } finally {
            // 根据状态分发到指定队列
            waittingToRemoveFromWaitingList.forEach(this::offerToSpecQueueByStatus);
            // 从 原队列移除 队列移除
            if (CollectionUtils.isNotEmpty(waittingToRemoveFromWaitingList)) wc.removeAll(wc.getWAITTING_TASK_QUEUE(), waittingToRemoveFromWaitingList);
        }
        log.info("doTaskDispatcherScheduler end.");
    }

    private void offerToSpecQueueByStatus(DQInstance instance) {
        DQInstanceStatus status = instance.getStatus();
        switch (status) {
            case WAITTING:
            case RUNNING:
            case SUBMITTING:
            case RECORDING:
                wc.offerToRecordingTaskQueue(instance);
                break;
            case EVALUATING:
                wc.offerToEvaluatingTaskQueue(instance);
                break;
            case EVALUATE_ALERTING:
            case FAILED_ALERTING:
                wc.offerToAlertingTaskQueue(instance);
                break;
            case FAILED:
                wc.addFailedDQInstanceInfo(instance);
                break;
            case SUCCESS:
                wc.addSuccessDQInstanceInfo(instance);
                break;
            default:
                // todo 未知状态 丢弃任务
                log.warn("Unknown status, id : {}, status : {}, instance: {}", instance.getId(), status, instance);
                break;
        }
    }

    @Scheduled(fixedDelay = 5 * 1000L)
    public void scanRecordingTask() {
        List<DQInstance> waittingToRemoveFromRecordingList = Lists.newArrayList();
        Queue<DQInstance> waitingToSubmitDQInstanceQueue = Queues.newPriorityBlockingQueue(wc.getRECORDING_TASK_QUEUE());
        try {
            while (CollectionUtils.isNotEmpty(waitingToSubmitDQInstanceQueue)) {
                try {
                    DQInstance dqInstance = waitingToSubmitDQInstanceQueue.poll();
                    if (dqInstance == null) break;
                    processRecordingInstance(dqInstance, waittingToRemoveFromRecordingList);
                } catch (Exception e) {
                    // todo
                    log.error("scanRecordingTask failed, ex:", e);
                }
            }
        } catch (Exception e) {
            // todo
            log.error("scanRecordingTask failed, ex:", e);
        } finally {
            // 根据状态分发到指定队列
            waittingToRemoveFromRecordingList.forEach(this::offerToSpecQueueByStatus);
            // 从 record 队列移除
            if (CollectionUtils.isNotEmpty(waittingToRemoveFromRecordingList)) wc.removeAll(wc.getRECORDING_TASK_QUEUE(), waittingToRemoveFromRecordingList);
        }
    }

    private void processRecordingInstance(DQInstance dqInstance, List<DQInstance> waittingToRemoveFromRecordingList) {
        // 检查当前环境是否有可以提交任务到dispatcher（并发度限制  需要根据提交的引擎计算）
        DQInstanceStatus instanceStatus = dqInstance.getStatus();
        List<DQBaseTask> subTaskList = dqInstance.getSubTaskList();
        switch (instanceStatus) {
            case ACCEPTED:
            case WAITTING:
                dqInstanceService.updateStatus(dqInstance, DQInstanceStatus.SUBMITTING);
                submitTaskToDispatcher(subTaskList);
                if (!dqInstance.hasTaskToSubmit()) {
                    dqInstanceService.updateStatus(dqInstance, DQInstanceStatus.RUNNING);
                }
                break;
            case SUBMITTING:
                submitTaskToDispatcher(subTaskList);
                // 检查是否所有任务都已经提交
                if (!dqInstance.hasTaskToSubmit()) {
                    dqInstanceService.updateStatus(dqInstance, DQInstanceStatus.RUNNING);
                }
                break;
            case RUNNING:
                // 检查并更新任务状态
                checkJobStatus(subTaskList);
                if (dqInstance.isFinishRecord()) {
                    // record 任务都完成了  准备移除
                    waittingToRemoveFromRecordingList.add(dqInstance);
                }
                break;
            default:
                break;
        }
    }

    private void checkJobStatus(List<DQBaseTask> subTaskList) {
        subTaskList.forEach(task -> dqTaskService.checkJobStatus(task));
    }

    private void submitTaskToDispatcher(List<DQBaseTask> subTaskList) {
        // 遍历 recording tasks 检查状态进行更新
        subTaskList.forEach(task -> {
            DQTaskStatus taskStatus = task.getStatus();
            switch (taskStatus) {
                case WAITTING:
                    // 提交任务
                    doSubmitTaskToDispatcher(task);
                    if (task.isFailed()) {
                        // 提交一直失败
                        dqTaskService.updateTaskStatus(task, DQTaskStatus.FAILED);
                    }
                    break;
                case RECORDING:
                    // 查询结果
                    boolean isFinished = dqTaskService.checkJobStatus(task);
                    if (isFinished) {
                        // 任务结束， 设置任务状态为record结束
                        dqTaskService.updateTaskStatus(task, DQTaskStatus.RECORDED);
                    }
                    break;
                default:
                    // 其余状态不处理
                    break;
            }
        });
    }

    private void doSubmitTaskToDispatcher(DQBaseTask task) {
        // 并发度检查
        if (!wc.canSubmitToSpecEngine(task.getEngine())) return;
        if (dqTaskService.doSubmitRecordingTask(task)) {
            // 任务提交成功  更新状态为recording
            dqTaskService.updateTaskStatus(task, DQTaskStatus.RECORDING);
        } else {
            // 提交失败 记录一次失败
            task.incrStatusAge();
        }
    }

    public void scanEvaluatingTask() {
        LinkedBlockingQueue<DQInstance> evaluating_task_queue = wc.getEVALUATING_TASK_QUEUE();
        while (true) {
            DQInstance dqInstance = null;
            try {
                // Evaluating 来一个处理一个
                dqInstance = evaluating_task_queue.poll(5, TimeUnit.SECONDS);
                if (dqInstance == null) continue;
                // 根据状态打回任务
                // 执行evaluating
                if (dqInstance.getStatus() == DQInstanceStatus.EVALUATING) {
                    dqInstance.doEvaluteTask();
                } else {
                    offerToSpecQueueByStatus(dqInstance);
                }
            } catch (Exception e) {
                if (dqInstance != null) {
                    log.error("scanEvaluatingTask doEvalute failed, id : {}， instance : {}, ex:", dqInstance.getId(), dqInstance, e);
                    dqInstanceService.updateStatus(dqInstance, DQInstanceStatus.FAILED_ALERTING);
                    offerToSpecQueueByStatus(dqInstance);
                } else {
                    log.error("scanEvaluatingTask poll instance failed. ex:", e);
                }
            }
        }
    }

    public void scanAlertingTask() {
        LinkedBlockingQueue<DQInstance> alerting_task_queue = wc.getALERTING_TASK_QUEUE();
        while (true) {
            DQInstance dqInstance = null;
            try {
                // Evaluating 来一个处理一个
                dqInstance = alerting_task_queue.poll(1, TimeUnit.SECONDS);
                if (dqInstance == null) continue;
                // 根据状态打回任务
                // 执行evaluating
                if (dqInstance.getStatus() == DQInstanceStatus.FAILED_ALERTING || dqInstance.getStatus() == DQInstanceStatus.EVALUATE_ALERTING) {
                    dqInstance.doAlertTask();
                } else {
                    offerToSpecQueueByStatus(dqInstance);
                }
            } catch (Exception e) {
                if (dqInstance != null) {
                    log.error("scanAlertingTask doAlert failed, id : {}， instance : {}, ex:", dqInstance.getId(), dqInstance, e);
                    if (dqInstance.isFailed()) {
                        // 重试很多次了 直接设置为失败
                        dqInstanceService.updateStatus(dqInstance, DQInstanceStatus.FAILED);
                        // 没有失败很多次的话，状态不修改 放回队列重试
                    }
                    // 放回队列准备重试
                    offerToSpecQueueByStatus(dqInstance);
                } else {
                    log.error("scanAlertingTask poll instance failed. ex:", e);
                }
            }
        }
    }
}
