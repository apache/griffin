package org.apache.griffin.api.entity.pojo;

import lombok.Data;
import org.apache.griffin.api.entity.enums.DQInstanceStatus;

/**
 * 任务实例， 当前任务运行时的快照
 *      一个实例包含多个子任务
 */
@Data
public class DQInstanceEntity implements Comparable<DQInstanceEntity> {

    private Long id;
    private Long dqcId;
    // 实例状态
    private DQInstanceStatus status;
    // 记录状态年龄  状态更新是重置
    private int statusAge;

    @Override
    public int compareTo(DQInstanceEntity o) {
        return 0;
    }
}
