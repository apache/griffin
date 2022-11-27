package org.apache.griffin.core.worker.entity.bo.task;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class DQKafkaTask extends DQBaseTask {
    @Override
    public List<Pair<Long, String>> getRecordInfo() {
        return null;
    }

}
