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


import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.job.entity.AbstractJob;
import org.apache.griffin.core.job.repo.JobRepo;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.metric.model.Metric;
import org.apache.griffin.core.metric.model.MetricValue;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class MetricServiceImpl implements MetricService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricServiceImpl.class);

    @Autowired
    private MeasureRepo<Measure> measureRepo;
    @Autowired
    private JobRepo<AbstractJob> jobRepo;
    @Autowired
    private MetricStore metricStore;

    @Override
    public List<Metric> getAllMetrics() {
        List<Metric> metrics = new ArrayList<>();
        List<AbstractJob> jobs = jobRepo.findByDeleted(false);
        List<Measure> measures = measureRepo.findByDeleted(false);
        Map<Long, Measure> measureMap = measures.stream().collect(Collectors.toMap(Measure::getId, Function.identity()));
        for (AbstractJob job : jobs) {
            List<MetricValue> metricValues = getMetricValues(job.getMetricName(), 0, 300);
            Measure measure = measureMap.get(job.getMeasureId());
            metrics.add(new Metric(job.getJobName(), measure.getDescription(), measure.getOrganization(), measure.getOwner(), metricValues));
        }
        return metrics;
    }

    @Override
    public List<MetricValue> getMetricValues(String metricName, int offset, int size) {
        try {
            return metricStore.getMetricValues(metricName, offset, size);
        } catch (Exception e) {
            LOGGER.error("Failed to get metric values named {}. {}", metricName, e.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public ResponseEntity addMetricValues(List<MetricValue> values) {
        for (MetricValue value : values) {
            if (!isMetricValueValid(value)) {
                LOGGER.error("Invalid metric value.");
                return new ResponseEntity<>(GriffinOperationMessage.ADD_METRIC_VALUES_FAIL, HttpStatus.BAD_REQUEST);
            }
        }
        try {
            for (MetricValue value : values) {
                metricStore.addMetricValue(value);
            }
            return new ResponseEntity<>(GriffinOperationMessage.ADD_METRIC_VALUES_SUCCESS, HttpStatus.CREATED);
        } catch (ResponseException e) {
            LOGGER.error("Failed to add metric values. {}", e.getMessage());
            HttpStatus status = HttpStatus.valueOf(e.getResponse().getStatusLine().getStatusCode());
            return new ResponseEntity<>(GriffinOperationMessage.ADD_METRIC_VALUES_FAIL, status);
        } catch (Exception e) {
            LOGGER.error("Failed to add metric values. {}", e.getMessage());
            return new ResponseEntity<>(GriffinOperationMessage.ADD_METRIC_VALUES_FAIL, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private boolean isMetricValueValid(MetricValue value) {
        return StringUtils.isNotBlank(value.getName()) && value.getTmst() != null && MapUtils.isNotEmpty(value.getValue());
    }

    @Override
    public ResponseEntity deleteMetricValues(String metricName) {
        try {
            metricStore.deleteMetricValues(metricName);
            return ResponseEntity.ok(GriffinOperationMessage.DELETE_METRIC_VALUES_SUCCESS);
        } catch (ResponseException e) {
            LOGGER.error("Failed to delete metric values named {}. {}", metricName, e.getMessage());
            HttpStatus status = HttpStatus.valueOf(e.getResponse().getStatusLine().getStatusCode());
            return new ResponseEntity<>(GriffinOperationMessage.DELETE_METRIC_VALUES_FAIL, status);
        } catch (Exception e) {
            LOGGER.error("Failed to delete metric values named {}. {}", metricName, e.getMessage());
            return new ResponseEntity<>(GriffinOperationMessage.DELETE_METRIC_VALUES_FAIL, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
