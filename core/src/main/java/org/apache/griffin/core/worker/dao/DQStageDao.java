package org.apache.griffin.core.worker.dao;

import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.stage.DQAbstractStage;
import org.apache.griffin.core.worker.stage.DQStage;

public interface DQStageDao {
    void updateDQStageStatus(DQStage stage, int status);

    void insert(DQAbstractStage stage);
}
