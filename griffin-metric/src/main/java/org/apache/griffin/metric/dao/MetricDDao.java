/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.metric.dao;

import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.mapper.MetricDMapper;
import org.apache.griffin.metric.entity.MetricD;
import org.apache.griffin.metric.exception.GriffinErr;
import org.apache.griffin.metric.exception.GriffinException;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class MetricDDao extends BaseDao<MetricD, MetricDMapper> {

    public static final String NOT_BE_NULL = "The metricD argument is illegal." +
            "Either metricD entity or metric name attribute must not be null.";

    public MetricDDao(MetricDMapper metricDMapper) {
        super(metricDMapper);
    }

    public int addMetricD(MetricD metricD) {
        validateEntity(metricD);
        int count = 0;
        try {
            count = mybatisMapper.insert(metricD);
        } catch (Exception e) {
            throw new GriffinException(e);
        }
        log.info("add metricD: {}, count: {}", metricD, count);
        return count;
    }

    private void validateEntity(MetricD metricD) {
        if (null == metricD || null == metricD.getMetricName()) {
            log.error(NOT_BE_NULL);
            throw new GriffinException(NOT_BE_NULL, GriffinErr.validationError);
        }
    }
}
