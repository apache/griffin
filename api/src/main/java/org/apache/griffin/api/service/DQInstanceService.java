package org.apache.griffin.api.service;

import org.apache.griffin.api.dao.DQInstanceDao;
import org.apache.griffin.api.entity.enums.DQInstanceStatus;
import org.apache.griffin.api.entity.pojo.DQInstanceEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DQInstanceService {
    private static final Logger log = LoggerFactory.getLogger(DQInstanceService.class);

    @Autowired
    private DQInstanceDao dqInstanceDao;
//    @Autowired
//    private DQContentInstanceMapDao dqContentInstanceMapDao;
//    @Autowired
//    private DQInstanceFactory dqInstanceFactory;

    /**
     * 数据库和内存需要保持同步
     * @param instance obj
     * @param status stat
     * @return true or false
     */
    public boolean updateStatus(DQInstanceEntity instance, DQInstanceStatus status) {
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
    public DQInstanceEntity getById(Long id) {
        if (id == null) {
            log.error("Unknown instance id: null");
        }
        DQInstanceEntity ins = dqInstanceDao.getById(id);
        return ins;
//        if (ins == null) {
//            // the ins id has no task info
//            return constructInstance(id);
//        }
//        return recoveryInstance(ins);
    }

//    private DQInstanceEntity constructInstance(Long id) {
//        log.info("constructInstance id: {}", id);
//        GriffinDQContentInstanceMap contentInstanceMap = dqContentInstanceMapDao.getContentInstanceMapByInstanceId(id);
//        Long instanceId = contentInstanceMap.getInstanceId();
//        Long dqcId = contentInstanceMap.getDqcId();
//        return dqInstanceFactory.constructInstance(instanceId, dqcId);
//    }
//
//    private DQInstance recoveryInstance(DQInstance instance) {
//        return dqInstanceFactory.recoveryInstance(instance);
//    }
}
