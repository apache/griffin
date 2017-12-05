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
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * In griffin, metricName usually equals to measureName, and we only save measureName in server.
 */

@RestController
@RequestMapping("/api/v1")
public class MetricController {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricController.class);
    @Autowired
    private MetricService metricService;
    @Autowired
    private MetricTemplateService templateService;

    @RequestMapping(value = "/metrics", method = RequestMethod.GET)
    public List<Metric> getAllMetrics() {
        return metricService.getAllMetrics();
    }
//
//    @RequestMapping(value = "metric", method = RequestMethod.GET)
//    public Metric getMetricByMetricName(@RequestParam("templateId") Long templateId) {
//        return metricService.getMetricByTemplateId(templateId);
//    }

    @RequestMapping(value = "metric", method = RequestMethod.GET)
    public Metric getMetricByMetricName(@RequestParam("metricName") String metricName) {
        return metricService.getMetricByMetricName(metricName);
    }

    @RequestMapping(value = "/metric/values", method = RequestMethod.GET)
    public List<MetricValue> getMetricValues(@RequestParam("metricName") String metricName) {
        return metricService.getMetricValues(metricName);
    }

    @RequestMapping(value = "/metric/values", method = RequestMethod.POST)
    public GriffinOperationMessage addMetricValues(@RequestBody List<MetricValue> values) {
        return metricService.addMetricValues(values);
    }

    @RequestMapping(value = "/metric/values", method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteMetricValues(@RequestParam("metricName") String metricName) {
        return metricService.deleteMetricValues(metricName);
    }

    @RequestMapping(value = "/metric/templates", method = RequestMethod.GET)
    public List<MetricTemplate> getAllTemplates() {
        return templateService.getAllTemplates();
    }

    @RequestMapping(value = "/metric/template/{id}", method = RequestMethod.GET)
    public MetricTemplate getTemplateById(@PathVariable("id") Long templateId) {
        return templateService.getTemplateById(templateId);
    }

    @RequestMapping(value = "/metric/template", method = RequestMethod.GET)
    public MetricTemplate getTemplateByMetricName(@RequestParam("metricName") String metricName) {
        return templateService.getTemplateByMetricName(metricName);
    }
}
