package org.apache.griffin.core.worker.service;

import org.apache.griffin.core.common.entity.GriffinDQContentInstanceMap;
import org.apache.griffin.core.common.dao.DQContentInstanceMapDao;
import org.apache.griffin.core.worker.dao.DQInstanceDao;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.enums.DQInstanceStatus;
import org.apache.griffin.core.worker.factory.DQInstanceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DQInstanceService {
    private static final Logger log = LoggerFactory.getLogger(DQInstanceService.class);

    @Autowired
    private DQInstanceDao dqInstanceDao;
    @Autowired
    private DQContentInstanceMapDao dqContentInstanceMapDao;
    @Autowired
    private DQInstanceFactory dqInstanceFactory;

    /**
     * 数据库和内存需要保持同步
     * @param instance obj
     * @param status stat
     * @return true or false
     */
    public boolean updateStatus(DQInstance instance, DQInstanceStatus status) {
        try {
            dqInstanceDao.updateDQInstanceStatus(instance, status.getCode());
            instance.setStatus(status);
            log.info("instance {} {} => {} success.", instance.getId(), instance.getStatus(), status);
            return true;
        } catch (Exception e) {
            log.error("instance {} {} => {} failed, ex", instance.getId(), instance.getStatus(), status, e);
        }
        return false;
    }

    /**
     * construct instance
     * @param id master assign id
     * @return DQInstance
     */
    public DQInstance getById(Long id) {
        if (id == null) {
            log.error("Unknown instance id: null");
        }
        DQInstance ins = dqInstanceDao.getById(id);
        if (ins == null) {
            // the ins id has no task info
            return constructInstance(id);
        }
        return recoveryInstance(ins);
    }

    private DQInstance constructInstance(Long id) {
        log.info("constructInstance id: {}", id);
        GriffinDQContentInstanceMap contentInstanceMap = dqContentInstanceMapDao.getContentInstanceMapByInstanceId(id);
        Long instanceId = contentInstanceMap.getInstanceId();
        Long dqcId = contentInstanceMap.getDqcId();
        return dqInstanceFactory.constructInstance(instanceId, dqcId);
    }

    private DQInstance recoveryInstance(DQInstance instance) {
        return dqInstanceFactory.recoveryInstance(instance);
    }
}
