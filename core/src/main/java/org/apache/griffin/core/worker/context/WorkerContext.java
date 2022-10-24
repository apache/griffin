package org.apache.griffin.core.worker.context;

import com.google.common.collect.Lists;
import org.apache.griffin.core.worker.entity.task.DQBaseTask;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * 上下文信息  全局唯一
 */
@Component
public class WorkerContext {

    public static final List<DQBaseTask> waittingTaskIdList = Lists.newCopyOnWriteArrayList();
//    public static final List<DQBaseTask> runningTaskIdList = Lists.newCopyOnWriteArrayList();
    // runningTaskIdList = RECORDING_TASK_LIST + EVALUATING_TASK_LIST + ALERTING_TASK_LIST
    public static final List<DQBaseTask> RECORDING_TASK_LIST = Lists.newCopyOnWriteArrayList();
    public static final List<DQBaseTask> EVALUATING_TASK_LIST = Lists.newCopyOnWriteArrayList();
    public static final List<DQBaseTask> ALERTING_TASK_LIST = Lists.newCopyOnWriteArrayList();

    // success和failed队列数据老化问题？
    public static final List<DQBaseTask> successTaskIdList = Lists.newCopyOnWriteArrayList();
    public static final List<DQBaseTask> failedTaskIdList = Lists.newCopyOnWriteArrayList();

    /**
     * 启动时，重置该机诶点运行的任务信息
     *      把所有该节点运行任务中 状态是 running和waitiing 的任务全部设置为失败
     */
    @PostConstruct
    public void resetTaskStatusWhenStartUp() {

    }

    // 统计当前节点任务信息
    public void getWorkerTaskStatus() {

    }

}
