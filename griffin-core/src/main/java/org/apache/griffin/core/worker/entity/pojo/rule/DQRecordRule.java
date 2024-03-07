package org.apache.griffin.core.worker.entity.pojo.rule;

import lombok.Data;
import org.apache.griffin.core.worker.entity.pojo.template.DQRecordTemplate;

@Data
public class DQRecordRule {
    private DQRecordTemplate template;
}
