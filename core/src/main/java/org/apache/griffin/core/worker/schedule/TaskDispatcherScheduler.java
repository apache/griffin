package org.apache.griffin.core.worker.schedule;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.commons.collections.CollectionUtils;
import org.apache.griffin.api.entity.enums.DQInstanceStatus;
import org.apache.griffin.core.common.utils.context.WorkerContext;
import org.apache.griffin.core.worker.entity.bo.DQInstanceBO;
import org.apache.griffin.core.worker.entity.enums.DQStageStatus;
import org.apache.griffin.core.worker.exception.StageSubmitException;
import org.apache.griffin.core.worker.service.DQStageService;
import org.apache.griffin.core.worker.service.DQWorkerInstanceService;
import org.apache.griffin.core.worker.stage.DQAbstractStage;
import org.apache.griffin.core.worker.stage.DQStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * schedule task
 */
@Component
public class TaskDispatcherScheduler {
    private static final Logger log = LoggerFactory.getLogger(TaskDispatcherScheduler.class);

    private WorkerContext wc;
    private DQWorkerInstanceService dqWorkerInstanceService;
    private DQStageService dqStageService;

    @Autowired
    public void setWc(WorkerContext wc) {
        this.wc = wc;
    }
    @Autowired
    public void setDqWorkerInstanceService(DQWorkerInstanceService dqWorkerInstanceService) {
        this.dqWorkerInstanceService = dqWorkerInstanceService;
    }
    @Autowired
    public void setDqStageService(DQStageService dqStageService) {
        this.dqStageService = dqStageService;
    }

    @PostConstruct
    public void startEvaluetingAndAlertThread() {
        // todo submit to thread pool
        scanAlertingTask();
        scanEvaluatingTask();
    }

    /**
     * schedule task
     * put task to the queue of recording task
     */
    @Scheduled(fixedDelay = 5 * 1000L)
    public void doTaskDispatcherScheduler() {
        log.info("doTaskDispatcherScheduler start.");
        List<DQInstanceBO> waittingToRemoveFromWaitingList = Lists.newArrayList();
        Queue<DQInstanceBO> waitingToRecordingDQInstanceQueue = Queues.newPriorityBlockingQueue(wc.getWAITTING_TASK_QUEUE());

        try {
            while (true) {
                try {
                    DQInstanceBO dqInstance = waitingToRecordingDQInstanceQueue.poll();
                    // queue is empty, quit
                    if (dqInstance == null) break;
                    if (DQInstanceStatus.ACCEPTED != dqInstance.getStatus()) {
                        // State is not init, sync from database and reassign it
                        dqInstance = dqWorkerInstanceService.getById(dqInstance.getId());
                        // assign task by status
                        waittingToRemoveFromWaitingList.add(dqInstance);
                    } else {
                        // normal, update status and remove from queue, then put it to the queue of recording task
                        if (dqWorkerInstanceService.updateStatus(dqInstance, DQInstanceStatus.WAITTING)) {
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
            // assign task by status
            waittingToRemoveFromWaitingList.forEach(this::offerToSpecQueueByStatus);
            // remove from queue
            if (CollectionUtils.isNotEmpty(waittingToRemoveFromWaitingList)) wc.removeAll(wc.getWAITTING_TASK_QUEUE(), waittingToRemoveFromWaitingList);
        }
        log.info("doTaskDispatcherScheduler end.");
    }

    private void offerToSpecQueueByStatus(DQInstanceBO instance) {
        DQInstanceStatus status = instance.getStatus();
        switch (status) {
            case WAITTING:
//            case RUNNING:
//            case SUBMITTING:
            case RECORDING:
                wc.offerToRecordingTaskQueue(instance);
                break;
            case EVALUATING:
                wc.offerToEvaluatingTaskQueue(instance);
                break;
//            case EVALUATE_ALERTING:
            case ALERTING:
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
                // todo Unknown state Drop
                log.warn("Unknown status, id : {}, status : {}, instance: {}", instance.getId(), status, instance);
                break;
        }
    }

    @Scheduled(fixedDelay = 5 * 1000L)
    public void scanRecordingTask() {
        List<DQInstanceBO> waittingToRemoveFromRecordingList = Lists.newArrayList();
        Queue<DQInstanceBO> waitingToSubmitDQInstanceQueue = Queues.newPriorityBlockingQueue(wc.getRECORDING_TASK_QUEUE());
        try {
            while (CollectionUtils.isNotEmpty(waitingToSubmitDQInstanceQueue)) {
                try {
                    DQInstanceBO dqInstance = waitingToSubmitDQInstanceQueue.poll();
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

    private void processRecordingInstance(DQInstanceBO dqInstance, List<DQInstanceBO> waittingToRemoveFromRecordingList) {
        try {
            DQAbstractStage recordingStage = dqInstance.getRecordingStage();
            DQStageStatus stageStatus = recordingStage.getStatus();
            if (stageStatus == DQStageStatus.RUNNABLE) {
                if (!dqStageService.submitStage(recordingStage)) {
                    throw new StageSubmitException("Submit stage failed!, instance id: " + dqInstance.getId());
                } else {
                    dqWorkerInstanceService.updateStatus(dqInstance, DQInstanceStatus.RECORDING);
                }
            } else if (stageStatus == DQStageStatus.FINISH) {
                // if there is one task success, the instance should be EVALUATING;
                // if all tasks are failed, the instance should be FAILED_ALERTING;
                DQInstanceStatus instanceStatus = recordingStage.hasSuccess()? DQInstanceStatus.EVALUATING: DQInstanceStatus.FAILED_ALERTING;
                dqWorkerInstanceService.updateStatus(dqInstance, instanceStatus);
                waittingToRemoveFromRecordingList.add(dqInstance);
            }
        } catch (Exception e) {
            // todo rollback dqInstance status
            log.error("e: ", e);
        }
    }

    public void scanEvaluatingTask() {
        Executors.newCachedThreadPool().execute(() -> {
            LinkedBlockingQueue<DQInstanceBO> evaluating_task_queue = wc.getEVALUATING_TASK_QUEUE();
            while (true) {
                DQInstanceBO dqInstance = null;
                try {
                    dqInstance = evaluating_task_queue.poll(5, TimeUnit.SECONDS);
                    if (dqInstance == null) continue;
                    // do Evaluate
                    DQStage evaluatingStage = dqInstance.getEvaluatingStage();
                    if (dqInstance.getStatus() == DQInstanceStatus.EVALUATING) {
                        dqStageService.executeStage(evaluatingStage);
                        DQInstanceStatus dqInstanceStatus = evaluatingStage.hasSuccess() ? DQInstanceStatus.ALERTING : DQInstanceStatus.FAILED_ALERTING;
                        dqWorkerInstanceService.updateStatus(dqInstance, dqInstanceStatus);
                    }
                    offerToSpecQueueByStatus(dqInstance);
                } catch (Exception e) {
                    if (dqInstance != null) {
                        log.error("scanEvaluatingTask doEvalute failed, id : {}， instance : {}, ex:", dqInstance.getId(), dqInstance, e);
                        dqWorkerInstanceService.updateStatus(dqInstance, DQInstanceStatus.FAILED_ALERTING);
                        offerToSpecQueueByStatus(dqInstance);
                    } else {
                        log.error("scanEvaluatingTask poll instance failed. ex:", e);
                    }
                }
            }
        });
    }

    public void scanAlertingTask() {
        Executors.newCachedThreadPool().execute(() -> {
            LinkedBlockingQueue<DQInstanceBO> alerting_task_queue = wc.getALERTING_TASK_QUEUE();
            while (true) {
                DQInstanceBO dqInstance = null;
                try {
                    dqInstance = alerting_task_queue.poll(1, TimeUnit.SECONDS);
                    if (dqInstance == null) continue;
                    // do alerting
                    if (dqInstance.getStatus() == DQInstanceStatus.FAILED_ALERTING || dqInstance.getStatus() == DQInstanceStatus.ALERTING) {
                        DQStage alertingStage = dqInstance.getAlertingStage();
                        dqStageService.executeStage(alertingStage);
                        DQInstanceStatus dqInstanceStatus = alertingStage.hasSuccess()? DQInstanceStatus.SUCCESS : DQInstanceStatus.FAILED;
                        dqWorkerInstanceService.updateStatus(dqInstance, dqInstanceStatus);
                    }
                    offerToSpecQueueByStatus(dqInstance);
                } catch (Exception e) {
                    if (dqInstance != null) {
                        log.error("scanAlertingTask doAlert failed, id : {}， instance : {}, ex:", dqInstance.getId(), dqInstance, e);
                        if (dqInstance.isFailed()) {
                            // retry 5 times, set failed
                            dqWorkerInstanceService.updateStatus(dqInstance, DQInstanceStatus.FAILED);
                            // retry times less than 5, do not modify status and put task back
                        }
                        // put task back
                        offerToSpecQueueByStatus(dqInstance);
                    } else {
                        log.error("scanAlertingTask poll instance failed. ex:", e);
                    }
                }
            }
        });
    }
}
