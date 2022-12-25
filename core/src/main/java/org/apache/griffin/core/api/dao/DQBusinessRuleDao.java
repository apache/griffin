package org.apache.griffin.core.api.dao;

import org.apache.griffin.core.api.entity.GriffinDQBusinessRule;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface DQBusinessRuleDao {
    List<GriffinDQBusinessRule> getListByDqcId(Long dqcId);
}
