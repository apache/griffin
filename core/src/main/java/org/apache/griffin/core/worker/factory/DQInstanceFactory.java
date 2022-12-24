package org.apache.griffin.core.worker.factory;

import org.apache.griffin.core.api.dao.DQContentDao;
import org.apache.griffin.core.api.entity.GriffinDQContent;
import org.apache.griffin.core.worker.dao.DQInstanceDao;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DQInstanceFactory {

    @Autowired
    private DQInstanceDao dqInstanceDao;
    @Autowired
    private DQContentDao dqContentDao;
    @Autowired
    private DQStageFactory dqStageFactory;

    public DQInstance constructInstance(Long id, Long dqcId) {
        DQInstance instance = new DQInstance();
        instance.setId(id);
        instance.setDqcId(dqcId);
        GriffinDQContent griffinDQContent = dqContentDao.getById(dqcId);
        // construct task
        // construct record stage
        // construct check stage
        // construct alert stage

        dqInstanceDao.insert(instance);
        return instance;
    }

}
