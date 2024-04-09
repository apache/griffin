package org.apache.griffin.core.model;

import java.util.Date;

public class GriffinTask {
    private Long taskId;
    private Long instanceId;
    private Integer taskNo;
    private Integer taskCount;
    private String taskParam;
    private Date executeStartTime;
    private Date executeEndTime;
    private Long executeDuration;
    private Integer executeState;
    private String executeSnapshot;
    private String worker;
    private Integer dispatchFailedCount;
    private String errorMsg;
    private Integer version;
}
