package org.apache.griffin.common.metrics;

import lombok.*;
import org.apache.griffin.common.model.BaseEntity;

@EqualsAndHashCode(callSuper = false)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class MetricD extends BaseEntity {
    private long metricid;
    private String metricname;
    private String own;
    private String description;
    /**
     * separate by ,
     * [tag,tag]
     */
    private String tags;
}
