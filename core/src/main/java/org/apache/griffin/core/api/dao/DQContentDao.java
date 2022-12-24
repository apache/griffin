package org.apache.griffin.core.api.dao;

import org.apache.griffin.core.api.entity.GriffinDQContent;

public interface DQContentDao {
    GriffinDQContent getById(Long id);
}
