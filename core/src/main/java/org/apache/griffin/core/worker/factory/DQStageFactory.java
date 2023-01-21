package org.apache.griffin.core.worker.factory;

import org.apache.griffin.core.common.utils.SpringUtils;
import org.apache.griffin.core.worker.dao.DQStageDao;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.enums.DQStageTypeEnum;
import org.apache.griffin.core.worker.service.DQStageService;
import org.apache.griffin.core.worker.service.DQTaskService;
import org.apache.griffin.core.worker.stage.DQAbstractStage;
import org.apache.griffin.core.worker.stage.DQAlertStage;
import org.apache.griffin.core.worker.stage.DQEvaluateStage;
import org.apache.griffin.core.worker.stage.DQRecordStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DQStageFactory {

    private static final Logger log = LoggerFactory.getLogger(DQStageFactory.class);

    @Autowired
    private DQStageDao dqStageDao;

    public DQAbstractStage constructStage(DQStageTypeEnum stageTypeEnum, DQInstance instance) {
        DQAbstractStage stage = null;
        switch (stageTypeEnum) {
            case RECORD:
                stage = new DQRecordStage();
                break;
            case EVALUATE:
                stage = new DQEvaluateStage();
                break;
            case ALERT:
                stage = new DQAlertStage();
                break;
            default:
                break;
        }
        if (stage == null) {
            log.error("DQStageFactory - Unknown Stage Type: {}", stageTypeEnum);
            return stage;
        }
        stage.setInstance(instance);
        stage.setSubTaskList(instance.getSubTaskList());
        stage.setDqTaskService(SpringUtils.getObject("dqTaskService", DQTaskService.class));
        stage.setDqStageService(SpringUtils.getObject("dqStageService", DQStageService.class));
        dqStageDao.insert(stage);
        return stage;
    }
}
