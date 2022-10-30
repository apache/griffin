package org.apache.griffin.core.worker.entity.dispatcher;

import lombok.Data;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;

@Data
public class SubmitRequest {
    private String recordSql;
    private DQEngineEnum engine;  // Spark,Hive,Presto,etc.
    private String owner;
    private Integer maxRetryCount;
}
