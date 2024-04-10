package org.apache.griffin.common.metrics;

import lombok.*;
import org.apache.griffin.common.model.BaseEntity;

import java.util.Date;

@EqualsAndHashCode(callSuper = false)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Data
public class MetricV extends BaseEntity{
    private static final long serialVersionUID = -2511050092597848773L;

    private long metricid;

    private long attemptid;

    private double val;
    /**
     * optional, [label=value,]
     */
    private String tags;


    @Builder(builderMethodName = "metricVBuilder") // Define custom builder method name
    public MetricV(long metricid, long attemptid, double val, String tags, Date mtime, Date ctime) {
        this.metricid = metricid;
        this.attemptid = attemptid;
        this.val = val;
        this.tags = tags;
        this.updatedAt = mtime;
        this.createdAt = ctime;
    }
}
