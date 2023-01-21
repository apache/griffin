package org.apache.griffin.core.common.dao;

import org.apache.griffin.core.common.entity.GriffinDQContentInstanceMap;
import org.springframework.stereotype.Component;

@Component
public interface DQContentInstanceMapDao {

    GriffinDQContentInstanceMap getContentInstanceMapByInstanceId(Long id);
}
