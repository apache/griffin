package org.apache.griffin.core.worker.entity.bo;

import lombok.Data;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;

import java.util.List;

/**
 * 任务实例， 当前任务运行时的快照
 *      一个实例包含多个子任务
 */
@Data
public class DQInstance {
    private Long id;
    // 实例状态
    private DQTaskStatus status;
    // 任务信息
    private List<DQBaseTask> subTaskList;
}
