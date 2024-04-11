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
        scanAlertingTask();
        scanEvaluatingTask();
    }

    /**
     */
    @Scheduled(fixedDelay = 5 * 1000L)
    public void doTaskDispatcherScheduler() {
        log.info("doTaskDispatcherScheduler start.");
        List<DQInstance> waittingToRemoveFromWaitingList = Lists.newArrayList();
        Queue<DQInstance> waitingToRecordingDQInstanceQueue = Queues.newPriorityBlockingQueue(wc.getWAITTING_TASK_QUEUE());
        // FETCH TASKS FROM WAITING QUEUE
        try {
            while (true) {
                try {
                    DQInstance dqInstance = waitingToRecordingDQInstanceQueue.poll();
                    if (dqInstance == null) break;
                    if (DQInstanceStatus.ACCEPTED != dqInstance.getStatus()) {
                        dqInstance = dqInstanceService.getById(dqInstance.getId());
                        waittingToRemoveFromWaitingList.add(dqInstance);
                    } else {
                        // submit to recording queue
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
            waittingToRemoveFromWaitingList.forEach(this::offerToSpecQueueByStatus);
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
            waittingToRemoveFromRecordingList.forEach(this::offerToSpecQueueByStatus);
            if (CollectionUtils.isNotEmpty(waittingToRemoveFromRecordingList)) wc.removeAll(wc.getRECORDING_TASK_QUEUE(), waittingToRemoveFromRecordingList);
        }
    }

    private void processRecordingInstance(DQInstance dqInstance, List<DQInstance> waittingToRemoveFromRecordingList) {
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
                if (!dqInstance.hasTaskToSubmit()) {
                    dqInstanceService.updateStatus(dqInstance, DQInstanceStatus.RUNNING);
                }
                break;
            case RUNNING:
                checkJobStatus(subTaskList);
                if (dqInstance.isFinishRecord()) {
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
        subTaskList.forEach(task -> {
            DQTaskStatus taskStatus = task.getStatus();
            switch (taskStatus) {
                case WAITTING:
                    doSubmitTaskToDispatcher(task);
                    if (task.isFailed()) {
                        dqTaskService.updateTaskStatus(task, DQTaskStatus.FAILED);
                    }
                    break;
                case RECORDING:
                    boolean isFinished = dqTaskService.checkJobStatus(task);
                    if (isFinished) {
                        dqTaskService.updateTaskStatus(task, DQTaskStatus.RECORDED);
                    }
                    break;
                default:
                    break;
            }
        });
    }

    private void doSubmitTaskToDispatcher(DQBaseTask task) {
        if (!wc.canSubmitToSpecEngine(task.getEngine())) return;
        if (dqTaskService.doSubmitRecordingTask(task)) {
            dqTaskService.updateTaskStatus(task, DQTaskStatus.RECORDING);
        } else {
            task.incrStatusAge();
        }
    }

    public void scanEvaluatingTask() {
        LinkedBlockingQueue<DQInstance> evaluating_task_queue = wc.getEVALUATING_TASK_QUEUE();
        while (true) {
            DQInstance dqInstance = null;
            try {
                // Evaluating
                dqInstance = evaluating_task_queue.poll(5, TimeUnit.SECONDS);
                if (dqInstance == null) continue;
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
                dqInstance = alerting_task_queue.poll(1, TimeUnit.SECONDS);
                if (dqInstance == null) continue;
                if (dqInstance.getStatus() == DQInstanceStatus.FAILED_ALERTING || dqInstance.getStatus() == DQInstanceStatus.EVALUATE_ALERTING) {
                    dqInstance.doAlertTask();
                } else {
                    offerToSpecQueueByStatus(dqInstance);
                }
            } catch (Exception e) {
                if (dqInstance != null) {
                    log.error("scanAlertingTask doAlert failed, id : {}， instance : {}, ex:", dqInstance.getId(), dqInstance, e);
                    if (dqInstance.isFailed()) {
                        dqInstanceService.updateStatus(dqInstance, DQInstanceStatus.FAILED);
                    }
                    offerToSpecQueueByStatus(dqInstance);
                } else {
                    log.error("scanAlertingTask poll instance failed. ex:", e);
                }
            }
        }
    }
}
