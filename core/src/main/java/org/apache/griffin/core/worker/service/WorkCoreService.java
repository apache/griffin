package org.apache.griffin.core.worker.service;

import org.apache.griffin.core.worker.context.WorkerContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class WorkCoreService {

    @Autowired
    private WorkerContext workerContext;

    public void submitDQTask(Long instanceId) {
        // 收到提交请求
        // 参数校验
        // 判断环境是否可以接收任务
        // 接收任务
    }

    public void acceptTask(Long instanceId) {
        // 构建任务实例
        // 提交队列等待执行
    }

    public void stopDQTask(Long instanceId) {

    }

    public void querySingleDQTask(Long instanceId) {

    }

}
