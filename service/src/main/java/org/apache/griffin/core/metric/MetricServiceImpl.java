/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.core.metric;


import org.apache.griffin.core.metric.domain.Metric;
import org.apache.griffin.core.metric.domain.MetricValue;
import org.apache.griffin.core.metric.entity.MetricTemplate;
import org.apache.griffin.core.metric.repo.MetricTemplateRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MetricServiceImpl implements MetricService {

    @Autowired
    private MetricStore metricStore;
    @Autowired
    private MetricTemplateRepo templateRepo;

    @Override
    public List<Metric> getAllMetrics() {
        List<Metric> metrics = new ArrayList<>();
        for (MetricTemplate template : templateRepo.findAll()) {
            metrics.add(getMetricByTemplate(template));
        }
        return metrics;
    }

    private Metric getMetricByTemplate(MetricTemplate template) {
        List<MetricValue> metricValues = getMetricValues(template.getMetricName());
        return new Metric(template.getName(), template.getDescription(), template.getOrganization(), template.getOwner(), metricValues);
    }

    @Override
    public List<MetricValue> getMetricValues(String metricName) {
        return metricStore.getMetricValues(metricName);
    }

    @Override
    public GriffinOperationMessage addMetricValues(List<MetricValue> values) {
        return metricStore.addMetricValues(values);
    }

    @Override
    public GriffinOperationMessage deleteMetricValues(String metricName) {
        return metricStore.deleteMetricValues(metricName);
    }
}
