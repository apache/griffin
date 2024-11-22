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

import org.apache.griffin.metric.entity.MetricD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class MetricDDaoTest extends BaseDaoTest{
    @Autowired
    private MetricDDao metricDDao;

    @Test
    void testAddMetric(){
        MetricD metricD= new MetricD();
        metricD.setOwner("sys");
        metricD.setMetricName("count");
        metricD.setDescription("table count for one partition");
        metricD.setMetricId(1L);

        metricDDao.addMetricD(metricD);

        List<MetricD> metircDs = metricDDao.queryAll();
        Assertions.assertNotEquals(0, metircDs.size());

    }
}
