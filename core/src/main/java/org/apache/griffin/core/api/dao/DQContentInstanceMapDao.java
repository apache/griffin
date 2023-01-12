package org.apache.griffin.core.api.dao;

import org.apache.griffin.core.api.entity.GriffinDQContentInstanceMap;
import org.springframework.stereotype.Component;

@Component
public interface DQContentInstanceMapDao {

    GriffinDQContentInstanceMap getContentInstanceMapByInstanceId(Long id);
}
