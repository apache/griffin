package org.apache.griffin.core.worker.service;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.griffin.core.worker.client.DispatcherClient;
import org.apache.griffin.core.worker.dao.DQTaskDao;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.dispatcher.*;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class DQTaskService {
    private static final Logger log = LoggerFactory.getLogger(DQTaskService.class);

    @Autowired
    private DQTaskDao dqTaskDao;
    @Autowired
    private DispatcherClient dispatcherClient;

    public boolean updateTaskStatus(DQBaseTask task, DQTaskStatus status) {
        try {
            dqTaskDao.updateDQTaskListStatus(task, status.getCode());
            task.setStatus(status);
            return true;
        } catch (Exception e) {
            log.error("task {} {} => {} failed, ex", task.getId(), task.getStatus(), status, e);
        }
        return false;
    }

    public boolean updateTaskStatus(List<DQBaseTask> tasks, DQTaskStatus status) {
        if (CollectionUtils.isEmpty(tasks)) return false;
        DQBaseTask sampleTask = tasks.get(0);
        List<Long> taskIdList = tasks.stream().map(DQBaseTask::getId).collect(Collectors.toList());
        try {
            dqTaskDao.updateDQTaskListStatus(tasks, status.getCode());
            tasks.forEach(x -> x.setStatus(status));
            return true;
        } catch (Exception e) {
            log.error("task {} {} => {} failed, ex", taskIdList, sampleTask.getStatus(), status, e);
        }
        return false;
    }

    public boolean doSubmitRecordingTask(DQBaseTask task) {
        // 一个task对应多个dispatcher任务 分别获取所有的任务对应的请求
        List<Pair<Long, SubmitRequest>> requestList = getSubmitRequest(task);
        // 提交任务 获取任务对应的job信息
        List<JobStatus> jobStatusList = requestList.stream()
                .map(reqPair -> Pair.of(reqPair.getLeft(), dispatcherClient.submitSql(reqPair.getRight())))
                .map(respPair -> dispatcherClient.wrapperSubmitResponse(respPair))
                .collect(Collectors.toList());
        // 设置job信息
        task.setJobStatusList(jobStatusList);
        return true;
    }

    public boolean checkJobStatus(DQBaseTask task) {
        List<JobStatus> jobStatusList = task.getJobStatusList();
        boolean isFinished = true;
        for (JobStatus jobStatus : jobStatusList) {
            if (jobStatus.isFinished()) continue;
            JobStatusRequest jobStatusRequest =  dispatcherClient.wrapperJobStatusRequest(jobStatus);
            JobStatusResponse jobStatusResponse = dispatcherClient.getJobStatus(jobStatusRequest);
            if (jobStatusResponse.isSuccess()) {
                // 更新状态 获取结果
                MetricRequest metricRequest = dispatcherClient.wrapperMetricRequest(jobStatus);
                MetricResponse metricResponse = dispatcherClient.getMetricResult(metricRequest);
                Double metric = metricResponse.getMetric();
                task.addMetric(jobStatus.getPartitionTime(), metric);
                jobStatus.setFinished(true);
            } else {
                // 只要有一个没有成功 就应该是false
                isFinished = false;
            }
        }
        return isFinished;
    }

    private List<Pair<Long, SubmitRequest>> getSubmitRequest(DQBaseTask task) {
        List<Pair<Long, String>> partitionTimeAndSqlList = task.record();
        return partitionTimeAndSqlList.stream()
                .map(partitionTimeAndSql -> {
                    Long paprtitionTime = partitionTimeAndSql.getLeft();
                    String recordSql = partitionTimeAndSql.getRight();
                    SubmitRequest request = SubmitRequest.builder()
                            .recordSql(recordSql)
                            .engine(task.getEngine())
                            .owner(task.getOwner())
                            .build();
                    return Pair.of(paprtitionTime, request);
                })
                .collect(Collectors.toList());
    }
}
