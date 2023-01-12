package org.apache.griffin.core.worker.dao;

import org.apache.griffin.core.worker.entity.bo.DQInstance;
import org.springframework.stereotype.Component;

@Component
public interface DQInstanceDao {

    DQInstance getById(Long id);

    void updateDQInstanceStatus(DQInstance instance, int status);

    void insert(DQInstance instance);
}
