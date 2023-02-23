package org.apache.griffin.core.master.service;

import org.apache.griffin.api.entity.enums.DQInstanceStatus;
import org.apache.griffin.api.dao.DQInstanceDao;
import org.apache.griffin.core.master.server.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DQTaskManagerInstanceService {
    private static final Logger log = LoggerFactory.getLogger(DQTaskManagerInstanceService.class);

    @Autowired
    private DQInstanceDao dqInstanceDao;
    @Autowired
    private TaskManager taskManager;

    public boolean stopTask(Long instanceId) {
        try {
            boolean stopResp = taskManager.stopDQTask(instanceId);
            if (stopResp) {
                dqInstanceDao.updateDQInstanceStatus(instanceId, DQInstanceStatus.STOPPED.getCode());
                log.info("instance {} STOPPED success.", instanceId);
                return true;
            } else {
                log.info("instance {} STOPPED failed.", instanceId);
                return false;
            }
        } catch (Exception e) {
            log.error("instance {} STOPPED failed, ex", instanceId, e);
        }
        return false;
    }
}
