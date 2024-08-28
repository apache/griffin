package org.apache.griffin.metric.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MetricTest {

    private MetricD metricD;
    private MetricV metricV1;
    private MetricV metricV2;
    private Tags tags;

    @BeforeEach
    public void setUp() {
        // Initialize MetricD
        metricD = MetricD.builder()
                .metricId(1L)
                .metricName("Metric A")
                .owner("Owner A")
                .description("Description A")
                .build();

        // Initialize MetricV
        metricV1 = MetricV.builder()
                .metricId(1L)
                .value(100.5)
                .tags(Tags.builder()
                        .metricId(1L)
                        .metricTags(createSampleTags())
                        .build())
                .build();

        metricV2 = MetricV.builder()
                .metricId(1L)
                .value(200.75)
                .tags(Tags.builder()
                        .metricId(1L)
                        .metricTags(createSampleTags())
                        .build())
                .build();

        // Initialize Tags
        tags = Tags.builder()
                .metricId(1L)
                .metricTags(createSampleTags())
                .build();
    }

    @Test
    public void testCreateMetricD() {
        assertNotNull(metricD);
        assertEquals(1L, metricD.getMetricId());
        assertEquals("Metric A", metricD.getMetricName());
        assertEquals("Owner A", metricD.getOwner());
        assertEquals("Description A", metricD.getDescription());
    }

    @Test
    public void testIngestMetricV() {
        List<MetricV> metricVs = new ArrayList<>();
        metricVs.add(metricV1);
        metricVs.add(metricV2);

        assertEquals(2, metricVs.size());
        assertTrue(metricVs.contains(metricV1));
        assertTrue(metricVs.contains(metricV2));
    }

    @Test
    public void testFetchMetricDWithTags() {
        // Mock fetch logic here. This would typically involve querying a database or service.
        MetricD fetchedMetricD = metricD;  // Simulate fetching
        Tags fetchedTags = tags;  // Simulate fetching tags

        assertNotNull(fetchedMetricD);
        assertEquals(1L, fetchedMetricD.getMetricId());

        assertNotNull(fetchedTags);
        assertEquals(1L, fetchedTags.getMetricId());
        assertEquals(2, fetchedTags.getMetricTags().size());
    }

    private List<MetricTag> createSampleTags() {
        List<MetricTag> tags = new ArrayList<>();
        tags.add(new MetricTag(1L, "key1", "value1"));
        tags.add(new MetricTag(2L, "key2", "value2"));
        return tags;
    }
}

