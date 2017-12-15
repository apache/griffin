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


import org.apache.griffin.core.job.entity.Job;
import org.apache.griffin.core.job.repo.JobRepo;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.metric.model.Metric;
import org.apache.griffin.core.metric.model.MetricValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class MetricServiceImpl implements MetricService {

    @Autowired
    private MeasureRepo<Measure> measureRepo;
    @Autowired
    private JobRepo<Job> jobRepo;
    @Autowired
    private MetricStore metricStore;

    @Override
    public List<Metric> getAllMetrics() {
        List<Metric> metrics = new ArrayList<>();
        List<Job> jobs = jobRepo.findByDeleted(false);
        List<Measure> measures = measureRepo.findByDeleted(false);
        Map<Long, Measure> measureMap = measures.stream().collect(Collectors.toMap(Measure::getId, Function.identity()));
        for (Job job : jobs) {
            List<MetricValue> metricValues = getMetricValues(job.getMetricName(), 300);
            Measure measure = measureMap.get(job.getMeasureId());
            metrics.add(new Metric(job.getName(), measure.getDescription(), measure.getOrganization(), measure.getOwner(), metricValues));
        }
        return metrics;
    }

    @Override
    public List<MetricValue> getMetricValues(String metricName, int size) {
        return metricStore.getMetricValues(metricName, size);
    }

    @Override
    public String addMetricValues(List<MetricValue> values) {
        return metricStore.addMetricValues(values);
    }

    @Override
    public String deleteMetricValues(String metricName) {
        return metricStore.deleteMetricValues(metricName);
    }
}
