package org.apache.griffin.metric.dao;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.mapper.MetricVMapper;
import org.apache.griffin.metric.entity.MetricV;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class MetricVDao extends BaseDao<MetricV, MetricVMapper>{
    public MetricVDao(@NonNull MetricVMapper mybatisMapper) {
        super(mybatisMapper);
    }

    public int addMetricV(MetricV metricV) {
        if (metricV == null) {
            log.warn("metricV is invalid");
            return 0;
        }
        int count = mybatisMapper.insert(metricV);
        log.info("add metricV: {}, count: {}", metricV, count);
        return count;
    }
}
