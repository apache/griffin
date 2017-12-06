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

    @Autowired
    private MetricService metricService;

    @RequestMapping(value = "/metrics", method = RequestMethod.GET)
    public List<Metric> getAllMetrics() {
        return metricService.getAllMetrics();
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
}
