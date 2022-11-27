package org.apache.griffin.core.worker.entity.pojo;

import lombok.Data;

import java.util.concurrent.TimeUnit;

@Data
public class DQTable {
    private Long id;
    private String dbName;
    private String tableName;
    private String partition;
    private TimeUnit unit;
}
