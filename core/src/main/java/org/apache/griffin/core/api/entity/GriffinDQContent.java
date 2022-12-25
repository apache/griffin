package org.apache.griffin.core.api.entity;

import lombok.Data;

/**
 * DQContent: one table has only one dqcContent
 */
@Data
public class GriffinDQContent {
    // id
    private Long id;
    private String owner;


    // table ID
    private Long resourceId;
    // tableName
    private String tableName;
    private DQResoueceEnum resoueceEnum;
}
