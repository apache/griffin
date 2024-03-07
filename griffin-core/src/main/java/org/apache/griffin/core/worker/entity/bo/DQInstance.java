package org.apache.griffin.core.worker.entity.bo;

import lombok.Data;
import org.apache.griffin.core.worker.entity.dispatcher.JobStatus;
import org.apache.griffin.core.worker.entity.enums.DQInstanceStatus;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;

import java.util.List;

/**
 * 任务实例， 当前任务运行时的快照
 *      一个实例包含多个子任务
 */
@Data
public class DQInstance {
    private Long id;
    // 实例状态
    private DQInstanceStatus status;
    // 记录状态年龄  状态更新是重置
    private int statusAge;
    // 任务信息
    private List<DQBaseTask> subTaskList;
    //
    private long scanTimeStamp = 0L;



    public void setStatus(DQInstanceStatus status) {
        if (this.status != status) resetStatusAge();
        this.status = status;
    }

    public void resetStatusAge() {
        statusAge = 0;
    }

    public void incrStatusAge() {
        statusAge++;
    }

    public boolean isFailed() {
        // 一个状态年龄处理了5次 无法变更 说明处理该任务一直失败
        return statusAge > 5;
    }

    public boolean hasTaskToSubmit() {
        boolean hasTaskToSubmit = false;
        for (DQBaseTask dqBaseTask : subTaskList) {
            if (dqBaseTask.getStatus() == DQTaskStatus.WAITTING) {
                hasTaskToSubmit = true;
                break;
            }
        }
        return hasTaskToSubmit;
    }

    public boolean isFinishRecord() {
        boolean isFinishRecord = true;
        for (DQBaseTask dqBaseTask : subTaskList) {
            if (dqBaseTask.getStatus().getCode() <= DQTaskStatus.RECORDING.getCode()) {
                isFinishRecord = false;
                break;
            }
        }
        return isFinishRecord;
    }

    public void doEvaluteTask() {
        subTaskList.forEach(DQBaseTask::evaluate);

    }

    public void doAlertTask() {
        // 收敛告警信息 进行告警
    }
}
