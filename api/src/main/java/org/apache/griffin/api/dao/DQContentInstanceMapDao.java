package org.apache.griffin.api.dao;

import org.apache.griffin.api.entity.GriffinDQContentInstanceMap;
import org.springframework.stereotype.Component;

@Component
public interface DQContentInstanceMapDao {

    GriffinDQContentInstanceMap getContentInstanceMapByInstanceId(Long id);
}
