package org.apache.griffin.core.worker.schedule;

import org.apache.griffin.core.worker.client.DispatcherClient;
import org.apache.griffin.core.worker.context.WorkerContext;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.dispatcher.*;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 任务执行调度期 和 dispatcher交互
 */
public class TaskDispatcherScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcherScheduler.class);
    @Autowired
    private WorkerContext wc;
    @Autowired
    private DispatcherClient dispatcherClient;

    /**
     * 进行任务调度
     */
    @Scheduled(fixedDelay = 1 * 5 * 1000L)
    public void doTaskDispatcherScheduler() {
        // 检查当前环境是否有可以提交任务到dispatcher（并发度限制）
        if (!wc.canSubmitToDispatcher()) return;
        // 从waitting队列获取任务
        DQInstance dqInstance = wc.getWaittingTask();
        // 开始提交任务 放到recording队列中
        dqInstance.setStatus(DQTaskStatus.WAITTING);
        wc.offerToRecordingTaskQueue(dqInstance);
    }

    private List<SubmitRequest> getSubmitRequest(DQInstance dqInstance) {

        return null;
    }

    @Scheduled(fixedDelay = 1 * 5 * 1000L)
    public void scanRecordingTask() {

        // 遍历 recording tasks 检查状态进行更新
        DQInstance dqInstance = wc.getRecordingTask();
        // recording队列任务应该只有两种状态  如果服用队列的话，可能有多种状态 先做分队列的方式
        switch (dqInstance.getStatus()) {
            case WAITTING:
                // 提交任务
                if (doSubmitRecordingTask(dqInstance)) {
                    dqInstance.setStatus(DQTaskStatus.RUNNING);
                    // 提交到队尾 等待下次处理
                    wc.offerToRecordingTaskQueue(dqInstance);
                } else {
                    // 提交失败
                    dqInstance.incrStatusAge();
                    if (dqInstance.isFailed()) {
                        wc.offerToAlertingTaskQueue(dqInstance);
                    } else {
                        // 如果失败 放回队尾 等待下次提交
                        wc.offerToRecordingTaskQueue(dqInstance);
                    }
                }
            case RUNNING:
                // 查询状态
                boolean hasRunningTask = checkJobStatus(dqInstance);
                if (hasRunningTask) {
                    // 提交到队尾 等待下次轮询
                    wc.offerToRecordingTaskQueue(dqInstance);
                } else {
                    // ? 是否拆任务提交到下一阶段 还是整体提交   先整体提交
                    // 没有正在运行的子任务了 提交给下一阶段执行
                    wc.offerToEvaluatingTaskQueue(dqInstance);
                }
        }
        // 如果状态是完成
            // 获取结果 放入task
            // 从recording tasks中移除任务
            // 放入到 evaluate 队列
        // 未完成  等待下次轮询
    }

    private boolean checkJobStatus(DQInstance dqInstance) {
        List<JobStatus> jobStatusList = dqInstance.getJobStatusList();
        boolean hasRunningTask = false;
        for (JobStatus jobStatus : jobStatusList) {
            if (jobStatus.isFinished()) continue;
            JobStatusRequest jobStatusRequest =  dispatcherClient.wrapperJobStatusRequest(jobStatus);
            JobStatusResponse jobStatusResponse = dispatcherClient.getJobStatus(jobStatusRequest);
            if (jobStatusResponse.isSuccess()) {
                jobStatus.setFinished(true);
            } else {
                hasRunningTask = true;
            }
        }
        return hasRunningTask;
    }

    private boolean doSubmitRecordingTask(DQInstance dqInstance) {
        // 一个task对应多个dispatcher任务 分别获取所有的任务对应的请求
        List<SubmitRequest> requestList = getSubmitRequest(dqInstance);
        // 提交任务 获取任务对应的job信息
        List<JobStatus> jobStatusList = requestList.stream()
                .map(req -> dispatcherClient.submitSql(req))
                .map(resp -> dispatcherClient.wrapperSubmitResponse(resp))
                .collect(Collectors.toList());
        // 设置job信息
        dqInstance.setJobStatusList(jobStatusList);
        return true;
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
