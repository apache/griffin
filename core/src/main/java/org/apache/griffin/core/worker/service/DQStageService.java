package org.apache.griffin.core.worker.service;

import org.apache.griffin.core.worker.dao.DQStageDao;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQStageStatus;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.apache.griffin.core.worker.stage.DQAbstractStage;
import org.apache.griffin.core.worker.stage.DQStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;

@Service
public class DQStageService {

    private static final Logger log = LoggerFactory.getLogger(DQStageService.class);
    private DQStageDao dqStageDao;

    public boolean updateTaskStatus(DQAbstractStage stage, DQStageStatus status) {
        try {
            dqStageDao.updateDQStageStatus(stage, status.getCode());
            stage.setStatus(status);
            return true;
        } catch (Exception e) {
            // todo
            log.error("stage {} {} => {} failed, ex", stage, stage.getStatus(), status, e);
        }
        return false;
    }

    // async to submit task
    public boolean submitStage(DQStage dqStage) {
        log.info("start to submit stage");
        // todo prepare thread pool
        try {
            Executors.newCachedThreadPool().execute(dqStage::start);
            return true;
        } catch (Exception e) {
            // todo
            log.error("Submit failed. ex:", e);
        }
        return false;
    }

    // sync to execute task
    public boolean executeStage(DQStage dqStage) {
        try {
            dqStage.start();
            return true;
        } catch (Exception e) {
            // todo
            log.error("Submit failed. ex:", e);
        }
        return false;
    }
}
