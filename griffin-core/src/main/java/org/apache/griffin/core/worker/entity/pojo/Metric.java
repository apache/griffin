package org.apache.griffin.core.worker.entity.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Metric {
    private long partitionTime;
    private double metric;
}
