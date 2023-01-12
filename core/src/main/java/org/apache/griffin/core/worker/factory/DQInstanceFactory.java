package org.apache.griffin.core.worker.factory;

import org.apache.griffin.core.api.dao.DQBusinessRuleDao;
import org.apache.griffin.core.api.dao.DQContentDao;
import org.apache.griffin.core.api.entity.GriffinDQBusinessRule;
import org.apache.griffin.core.api.entity.GriffinDQContent;
import org.apache.griffin.core.worker.dao.DQInstanceDao;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQStageTypeEnum;
import org.apache.griffin.core.worker.stage.DQAbstractStage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DQInstanceFactory {

    @Autowired
    private DQInstanceDao dqInstanceDao;
    @Autowired
    private DQContentDao dqContentDao;
    @Autowired
    private DQBusinessRuleDao dqBusinessRuleDao;
    @Autowired
    private DQStageFactory dqStageFactory;
    @Autowired
    private DQTaskFactory dqTaskFactory;

    public DQInstance constructInstance(Long id, Long dqcId) {
        DQInstance instance = new DQInstance();
        instance.setId(id);
        instance.setDqcId(dqcId);
        GriffinDQContent griffinDQContent = dqContentDao.getById(dqcId);
        // construct taskL
        List<GriffinDQBusinessRule> businessRuleList = dqBusinessRuleDao.getListByDqcId(dqcId);
        List<DQBaseTask> subTaskList = dqTaskFactory.constructTasks(griffinDQContent.getResoueceEnum(), businessRuleList);
        instance.setSubTaskList(subTaskList);
        // construct record stage
        DQAbstractStage reordStage = dqStageFactory.constructStage(DQStageTypeEnum.RECORD, instance);
        instance.setRecordingStage(reordStage);
        // construct check stage
        DQAbstractStage evaluateStage = dqStageFactory.constructStage(DQStageTypeEnum.EVALUATE, instance);
        instance.setEvaluatingStage(evaluateStage);
        // construct alert stage
        DQAbstractStage alertStage = dqStageFactory.constructStage(DQStageTypeEnum.ALERT, instance);
        instance.setAlertingStage(alertStage);
        dqInstanceDao.insert(instance);
        return instance;
    }

    public DQInstance recoveryInstance(DQInstance instance) {
        return null;
    }
}
