package org.apache.griffin.core.worker.dao;

import org.apache.griffin.core.api.entity.GriffinDQContentInstanceMap;
import org.springframework.stereotype.Component;

@Component
public interface DQContentInstanceMapDao {

    GriffinDQContentInstanceMap getContentInstanceMapByInstanceId(Long id);
}
