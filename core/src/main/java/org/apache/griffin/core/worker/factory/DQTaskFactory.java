package org.apache.griffin.core.worker.factory;

import org.apache.griffin.api.entity.DQResoueceEnums;
import org.apache.griffin.api.entity.GriffinDQBusinessRule;
import org.apache.griffin.core.worker.dao.DQTaskDao;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class DQTaskFactory {

    private static final Logger log = LoggerFactory.getLogger(DQTaskFactory.class);

    @Autowired
    private DQTaskDao dqTaskDao;

    public List<DQBaseTask> constructTasks(DQResoueceEnums resoueceEnum,
                                           List<GriffinDQBusinessRule> businessRuleList) {
        switch (resoueceEnum) {
            // construct hive
            case HIVE: return constructHiveTasks(businessRuleList);
            // construct kafka
            case KAFKA: return constructKafkaTasks(businessRuleList);
        }
        return null;
    }

    private List<DQBaseTask> constructHiveTasks(List<GriffinDQBusinessRule> businessRuleList) {
        return businessRuleList.stream()
                .map(this::constructHiveTask)
                .collect(Collectors.toList());
    }

    private DQBaseTask constructHiveTask(GriffinDQBusinessRule businessRule) {
        return null;
    }

    private List<DQBaseTask> constructKafkaTasks(List<GriffinDQBusinessRule> businessRuleList) {
        return businessRuleList.stream()
                .map(this::constructKafkaTask)
                .collect(Collectors.toList());
    }

    private DQBaseTask constructKafkaTask(GriffinDQBusinessRule businessRule) {
        // todo
        return null;
    }

}
