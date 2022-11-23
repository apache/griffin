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
        // 构建任务实例 怎么保证实例生成的ID一样？ 1 实例ID由master指派  2 entity id + partition 3 实例由master生成
            // 构建新实例
            // 恢复实例 （任务重新分配过来的）
        // 根据状态提交队列等待执行
            // 新构建的实例 -> waitting 等待调度
            // 恢复的实例  根据状态调度
    }

    public void stopDQTask(Long instanceId) {

    }

    public void querySingleDQTask(Long instanceId) {

    }

}
