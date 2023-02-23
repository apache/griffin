package org.apache.griffin.api.entity.pojo.rule;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;
import org.apache.griffin.api.entity.pojo.DQTable;
import org.apache.griffin.core.worker.entity.template.DQRecordBaseTemplate;
import org.apache.griffin.core.worker.entity.template.DQRecordTemplate;
import org.apache.griffin.core.worker.utils.DQDateUtils;

import java.util.List;
import java.util.Map;

@Data
public class DQRecordRule {

    private Long id;
    private Map<String, String> baseTemplateParams;
    private DQRecordTemplate template;
    private DQTable table;


    public List<Pair<Long, String>> getPartitionAndRuleIdList(Long businessTime, DQEngineEnum engine) {
        // prepare params
        List<Pair<Long, String>> partitionAndRuleIdList = Lists.newArrayList();
        if (template instanceof DQRecordBaseTemplate) {
            // base template
            baseTemplateParams.put("partition_sql_info", DQDateUtils.praseBussinesstimeToPartitionSQL(businessTime, table.getPartition()));
            String recordSql = template.getRecordSql(baseTemplateParams, engine);
            partitionAndRuleIdList.add(Pair.of(businessTime, recordSql));
        } else {
            List<Integer> offsetList = getOffsetList();
            offsetList.forEach(offset -> {
                Map<String, String> templateParams = Maps.newHashMap(baseTemplateParams);
                Long offsetBusinessTime = DQDateUtils.offsetTime(businessTime, offset, table.getUnit());
                baseTemplateParams.put("partition_sql_info", DQDateUtils.praseBussinesstimeToPartitionSQL(offsetBusinessTime, table.getPartition()));
                String recordSql = template.getRecordSql(templateParams, engine);
                partitionAndRuleIdList.add(Pair.of(offsetBusinessTime, recordSql));
            });
        }
        return partitionAndRuleIdList;
    }

    private List<Integer> getOffsetList() {
        return null;
    }
}
