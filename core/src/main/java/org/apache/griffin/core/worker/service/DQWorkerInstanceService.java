package org.apache.griffin.core.worker.service;

import org.apache.griffin.api.dao.DQInstanceDao;
import org.apache.griffin.api.entity.GriffinDQContentInstanceMap;
import org.apache.griffin.api.entity.enums.DQInstanceStatus;
import org.apache.griffin.api.entity.pojo.DQInstanceEntity;
import org.apache.griffin.api.dao.DQContentInstanceMapDao;
import org.apache.griffin.core.worker.entity.bo.DQInstanceBO;
import org.apache.griffin.core.worker.factory.DQInstanceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DQWorkerInstanceService {
    private static final Logger log = LoggerFactory.getLogger(DQWorkerInstanceService.class);

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
    public boolean updateStatus(DQInstanceBO instance, DQInstanceStatus status) {
        try {
            // todo  report to master
            dqInstanceDao.updateDQInstanceStatus(instance.getId(), status.getCode());
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
    public DQInstanceBO getById(Long id) {
        if (id == null) {
            log.error("Unknown instance id: null");
        }
        DQInstanceEntity ins = dqInstanceDao.getById(id);
        if (ins == null) {
            // the ins id has no task info
            DQInstanceBO dqInstanceBO = constructInstance(id);
            saveDqcInstanceEntityFromBO(dqInstanceBO);
            return dqInstanceBO;
        }
        return recoveryInstance(ins);
    }

    private void saveDqcInstanceEntityFromBO(DQInstanceBO dqInstanceBO) {
    }

    private DQInstanceBO constructInstance(Long id) {
        log.info("constructInstance id: {}", id);
        GriffinDQContentInstanceMap contentInstanceMap = dqContentInstanceMapDao.getContentInstanceMapByInstanceId(id);
        Long instanceId = contentInstanceMap.getInstanceId();
        Long dqcId = contentInstanceMap.getDqcId();
        return dqInstanceFactory.constructInstance(instanceId, dqcId);
    }

    private DQInstanceBO recoveryInstance(DQInstanceEntity instance) {
        return dqInstanceFactory.recoveryInstance(instance);
    }
}
