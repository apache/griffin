package org.apache.griffin.core.worker.stage;

import org.apache.griffin.core.worker.client.DispatcherClient;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQStageStatus;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DQRecordStage extends DQAbstractStage {
    private static final Logger log = LoggerFactory.getLogger(DQRecordStage.class);

    private DispatcherClient dispatcherClient;


    public DispatcherClient getDispatcherClient() {
        return dispatcherClient;
    }

    public void setDispatcherClient(DispatcherClient dispatcherClient) {
        this.dispatcherClient = dispatcherClient;
    }


    @Override
    public void process() {
        while (continueCheckTaskStaus()) {
            try {
                submitTaskToDispatcher();
                TimeUnit.SECONDS.sleep(1);
            } catch (Exception e) {
                // todo
                log.error("ex: ", e);
            }
        }
    }

    /**
     * one of tasks submitted to dispatcher is success
     * @return boolean
     */
    @Override
    public boolean hasSuccess() {
        for (DQBaseTask task : subTaskList) {
            if (task.getStatus() == DQTaskStatus.RECORDED) {
                return true;
            }
        }
        return false;
    }

    public boolean continueCheckTaskStaus() {
        boolean doContinue = false;
        for (DQBaseTask dqBaseTask : subTaskList) {
            if (dqBaseTask.getStatus() == DQTaskStatus.RECORDING || dqBaseTask.getStatus() == DQTaskStatus.WAITTING) {
                doContinue = true;
                break;
            }
        }
        return doContinue;
    }

    private void submitTaskToDispatcher() {
        // loop and check status
        subTaskList.forEach(task -> {
            DQTaskStatus taskStatus = task.getStatus();
            switch (taskStatus) {
                case WAITTING:
                    // submit task
                    doSubmitTaskToDispatcher(task);
                    if (task.isFailed()) {
                        // failed
                        dqTaskService.updateTaskStatus(task, DQTaskStatus.FAILED);
                    }
                    break;
                case RECORDING:
                    // query status
                    boolean isFinished = dqTaskService.checkJobStatus(task);
                    if (isFinished) {
                        // task is finish, set status
                        dqTaskService.updateTaskStatus(task, DQTaskStatus.RECORDED);
                    }
                    break;
                default:
                    // no handle
                    break;
            }
        });
    }

    private void doSubmitTaskToDispatcher(DQBaseTask task) {
        // 并发度检查
        if (!dispatcherClient.canSubmitToSpecEngine(task.getEngine())) return;
        if (dqTaskService.doSubmitRecordingTask(task)) {
            // 任务提交成功  更新状态为recording
            dqTaskService.updateTaskStatus(task, DQTaskStatus.RECORDING);
        } else {
            // 提交失败 记录一次失败
            task.incrStatusAge();
        }
    }
}
