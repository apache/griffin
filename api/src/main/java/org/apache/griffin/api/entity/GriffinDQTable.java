package org.apache.griffin.api.entity;

import lombok.Data;

/**
 * Table info
 */
@Data
public class GriffinDQTable {
    private Long id;
    private String tableName;
    private DQResoueceEnums resoueceEnum;
}
