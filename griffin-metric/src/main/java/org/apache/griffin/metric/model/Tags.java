package org.apache.griffin.metric.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * A common tag entity represents the relationships among metric entities and metric tag entities.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Tags extends BaseEntity {

    /**
     * Metric entity's identity.
     */
    private long metricId;

    /**
     * All tag properties assigning to a metric entity.
     */
    private List<MetricTag> metricTags;
}
