package org.apache.griffin.core.worker.entity.bo.task;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.griffin.core.worker.entity.pojo.rule.DQRecordRule;

import java.util.List;

/**
 * 表维度
 */
public class DQHiveTask extends DQBaseTask {

    @Override
    public List<Pair<Long, String>> getRecordInfo() {
        return recordRule.getPartitionAndRuleIdList(businessTime, engine);
    }

}
