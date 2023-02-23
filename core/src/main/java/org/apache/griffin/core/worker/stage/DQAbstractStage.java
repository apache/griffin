package org.apache.griffin.core.worker.stage;

import org.apache.griffin.core.worker.entity.bo.DQInstanceBO;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQStageStatus;
import org.apache.griffin.core.worker.service.DQStageService;
import org.apache.griffin.core.worker.service.DQTaskService;

import java.util.List;

public abstract class DQAbstractStage implements DQStage {
    protected DQTaskService dqTaskService;
    protected DQStageService dqStageService;

    protected DQStageStatus status;
    protected DQInstanceBO instance;

    protected List<DQBaseTask> subTaskList;

    public DQAbstractStage() {
        this.status = DQStageStatus.INIT;
    }

    public DQInstanceBO getInstance() {
        return instance;
    }

    public void setInstance(DQInstanceBO instance) {
        this.instance = instance;
    }

    public void setDqTaskService(DQTaskService dqTaskService) {
        this.dqTaskService = dqTaskService;
    }

    public void setDqStageService(DQStageService dqStageService) {
        this.dqStageService = dqStageService;
    }

    public List<DQBaseTask> getSubTaskList() {
        return subTaskList;
    }

    public void setSubTaskList(List<DQBaseTask> subTaskList) {
        this.subTaskList = subTaskList;
    }

    public DQStageStatus getStatus() {
        return status;
    }

    public void setStatus(DQStageStatus status) {
        this.status = status;
    }

    public abstract void process();

    public void updateStatus(DQStageStatus status) {
        dqStageService.updateTaskStatus(this, status);
    }

    public void start() {
        updateStatus(DQStageStatus.RUNNING);
        process();
        updateStatus(DQStageStatus.FINISH);
    }
}
