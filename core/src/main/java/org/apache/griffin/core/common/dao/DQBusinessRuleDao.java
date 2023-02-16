package org.apache.griffin.core.common.dao;

import org.apache.griffin.core.common.entity.GriffinDQBusinessRule;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface DQBusinessRuleDao {
    List<GriffinDQBusinessRule> getListByDqcId(Long dqcId);
}
