package org.apache.griffin.core.worker.entity.bo;

import lombok.Data;
import org.apache.griffin.api.entity.enums.DQInstanceStatus;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.apache.griffin.core.worker.entity.pojo.rule.DQAlertRule;
import org.apache.griffin.core.worker.stage.DQAbstractStage;

import java.util.List;

/**
 * 任务实例， 当前任务运行时的快照
 *      一个实例包含多个子任务
 */
@Data
public class DQInstance implements Comparable<DQInstance> {
    private Long id;

    private Long dqcId;

    // 实例状态
    private DQInstanceStatus status;
    // 记录状态年龄  状态更新是重置
    private int statusAge;
    // 任务信息
    private List<DQBaseTask> subTaskList;
    //
    private long scanTimeStamp = 0L;

    protected DQAlertRule dqAlertRule;

    private DQAbstractStage recordingStage;
    private DQAbstractStage evaluatingStage;
    private DQAbstractStage alertingStage;



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

    @Override
    public int compareTo(DQInstance o) {
        return 0;
    }
}
