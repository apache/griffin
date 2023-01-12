package org.apache.griffin.core.worker.stage;

import org.apache.griffin.core.worker.entity.enums.DQStageStatus;

public interface DQStage {
    void process();
    void start();
    boolean hasSuccess();
}
