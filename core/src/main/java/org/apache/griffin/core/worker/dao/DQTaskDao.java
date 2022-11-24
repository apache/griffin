package org.apache.griffin.core.worker.dao;

import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface DQTaskDao {

    void updateDQTaskListStatus(List<DQBaseTask> tasks, int status);
    void updateDQTaskListStatus(DQBaseTask task, int status);

}
