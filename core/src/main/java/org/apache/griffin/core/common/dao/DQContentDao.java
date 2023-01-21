package org.apache.griffin.core.common.dao;


import org.apache.griffin.core.common.entity.GriffinDQContent;

public interface DQContentDao {
    GriffinDQContent getById(Long id);
}
