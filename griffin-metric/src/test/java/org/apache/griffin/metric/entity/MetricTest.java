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
package org.apache.griffin.metric.entity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MetricTest {

    public static final long METRIC_A_ID = 100L;
    public static final long VALUE_ID_1 = 1001L;
    public static final long VALUE_ID_2 = 1002L;
    public static final String METRIC_A_NAME = "Metric A";
    public static final String OWNER_A = "Owner A";
    public static final String DESCRIPTION_A = "Description A";
    private MetricD metricD;
    public static final long PERFMETRIC_TAGD_ID = 5000L;
    public static final long CAPACITYMETRIC_TAGD_ID = 5001L;
    private MetricTagD perfMetricTagD, capacityMetricTagD;
    private MetricV metricV1;
    private MetricV metricV2;

    @BeforeEach
    public void setUp() {
        // Initialize MetricD
        metricD = MetricD.builder()
                .metricId(METRIC_A_ID)
                .metricName(METRIC_A_NAME)
                .owner(OWNER_A)
                .description(DESCRIPTION_A)
                .build();

        // Initialize MetricV
        metricV1 = MetricV.builder()
                .id(VALUE_ID_1)
                .metricId(METRIC_A_ID)
                .value(100.5)
                .tags(createSampleTags())
                .build();

        metricV2 = MetricV.builder()
                .id(VALUE_ID_2)
                .metricId(METRIC_A_ID)
                .value(200.75)
                .tags(createSampleTags())
                .build();
    }

    @Test
    public void testCreateMetricD() {
        assertNotNull(metricD);
        assertEquals(METRIC_A_ID, metricD.getMetricId());
        assertEquals(METRIC_A_NAME, metricD.getMetricName());
        assertEquals(OWNER_A, metricD.getOwner());
        assertEquals(DESCRIPTION_A, metricD.getDescription());
    }

    @Test
    public void testIngestMetricV() {
        List<MetricV> metricVs = new ArrayList<>();
        metricVs.add(metricV1);
        metricVs.add(metricV2);

        assertEquals(2, metricVs.size());
        assertTrue(metricVs.contains(metricV1));
        assertTrue(metricVs.contains(metricV2));
        assertEquals(metricV1.getMetricId(), metricV2.getMetricId());
        assertEquals(metricD.getMetricId(), metricV1.getMetricId());
        assertEquals(metricD.getMetricId(), metricV2.getMetricId());
    }

    @Test
    public void testFetchMetricDWithTags() {
        // Mock fetch logic here. This would typically involve querying a database or service.
        MetricD fetchedMetricD = metricD;  // Simulate fetching
        List<MetricTagD> fetchedTagAttachment = metricV1.getTags();

        assertNotNull(fetchedMetricD);
        assertEquals(METRIC_A_ID, fetchedMetricD.getMetricId());

        assertNotNull(fetchedTagAttachment);
        assertEquals(2, fetchedTagAttachment.size());
    }

    private List<MetricTagD> createSampleTags() {
        List<MetricTagD> tags = new ArrayList<>();

        // Initialize MetricTagD
        perfMetricTagD = MetricTagD.builder()
                .id(PERFMETRIC_TAGD_ID)
                .tagKey("perf")
                .tagValue("baseline")
                .build();
        capacityMetricTagD = MetricTagD.builder()
                .id(CAPACITYMETRIC_TAGD_ID)
                .tagKey("capacity")
                .tagValue("overall")
                .build();

        tags.add(perfMetricTagD);
        tags.add(capacityMetricTagD);
        return tags;
    }
}

