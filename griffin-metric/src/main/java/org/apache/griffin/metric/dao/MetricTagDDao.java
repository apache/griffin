package org.apache.griffin.metric.dao;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.mapper.MetricTagDMapper;
import org.apache.griffin.metric.entity.MetricTagD;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class MetricTagDDao extends BaseDao<MetricTagD, MetricTagDMapper>{

    public MetricTagDDao(@NonNull MetricTagDMapper mybatisMapper) {
        super(mybatisMapper);
    }

    public int addMetricTagD(MetricTagD metricTagD) {
        if (metricTagD == null) {
            log.warn("metricTagD is invalid");
            return 0;
        }

        int count = mybatisMapper.insert(metricTagD);
        log.info("add metricTagD: {}, count: {}", metricTagD, count);
        return count;
    }
}
