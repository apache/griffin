package org.apache.griffin.api.dao;


import org.apache.griffin.api.entity.GriffinDQContent;

public interface DQContentDao {
    GriffinDQContent getById(Long id);
}
