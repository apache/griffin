package org.apache.griffin.core.worker.schedule;

import org.apache.griffin.core.worker.context.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;

/**
 * 任务执行调度期 和 dispatcher交互
 */
public class TaskDispatcherScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcherScheduler.class);
    @Autowired
    private WorkerContext wc;

    /**
     * 进行任务调度
     */
    public void doTaskDispatcherScheduler() {
        // 检查当前环境是否有可以提交任务到dispatcher（并发度限制）
        // 从waitting队列获取任务
        // 开始提交任务
        // 放到recording队列中
    }

    public void scanRecordingTask() {
        // 遍历 recording tasks 检查状态进行更新
        // 如果状态是完成
            // 获取结果 放入task
            // 从recording tasks中移除任务
            // 放入到 evaluate 队列
        // 未完成  等待下次轮询
    }

    public void scanEvaluatingTask() {
        // 遍历 evaluate 队列
        // 如果状态是完成
        // 获取结果 放入task
        // 从evaluate tasks中移除任务
        // 放入到alert队列
        // 未完成  等待下次轮询
    }

    public void scanAlertingTask() {
        // 遍历 evaluate 队列
        // 如果状态是完成
        // 获取结果 放入task
        // 从evaluate tasks中移除任务
        // 放入到alert队列
        // 未完成  等待下次轮询
    }

}
