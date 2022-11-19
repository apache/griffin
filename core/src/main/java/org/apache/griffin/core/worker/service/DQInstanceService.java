package org.apache.griffin.core.worker.service;

import org.apache.griffin.core.worker.dao.DQInstanceDao;
import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.apache.griffin.core.worker.entity.enums.DQInstanceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DQInstanceService {
    private static final Logger log = LoggerFactory.getLogger(DQInstanceService.class);

    @Autowired
    private DQInstanceDao dqInstanceDao;

    /**
     * 数据库和内存需要保持同步
     * @param instance
     * @param status
     * @return
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

    public DQInstance getById(long id) {
        return null;
    }
}
