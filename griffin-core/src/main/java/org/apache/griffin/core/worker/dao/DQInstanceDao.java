package org.apache.griffin.core.worker.dao;

import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.springframework.stereotype.Component;

@Component
public interface DQInstanceDao {

    void updateDQInstanceStatus(DQInstance instance, int status);

}
