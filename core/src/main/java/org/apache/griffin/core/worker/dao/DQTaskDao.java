package org.apache.griffin.core.worker.dao;

import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface DQTaskDao {

    void updateDQTaskListStatus(List<DQBaseTask> tasks, int status);
    void updateDQTaskStatus(DQBaseTask task, int status);

}
