package org.apache.griffin.api.dao;

import org.apache.griffin.api.entity.pojo.DQInstanceEntity;
import org.springframework.stereotype.Component;

@Component
public interface DQInstanceDao {

    DQInstanceEntity getById(Long id);

    void updateDQInstanceStatus(Long id, int status);

    void insert(DQInstanceEntity instance);
}
