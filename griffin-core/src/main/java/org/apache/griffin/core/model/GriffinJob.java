package org.apache.griffin.core.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.griffin.common.model.BaseEntity;

import java.util.Date;

@Getter
@Setter
public class GriffinJob extends BaseEntity {
    private Long jobId;
    private String group;
    private String jobName;
    private Integer jobType;
    private Integer jobState;
    private String jobHandler;
    private String jobParam;
    private Integer retryType;
    private Integer retryCount;
    private Integer retryInterval;
    private Date startTime;
    private Date endTime;
    private Integer triggerType;
    private String triggerValue;
    private Integer executeTimeout;
    private Integer collidedStrategy;
    private Integer misfireStrategy;
    private Integer routeStrategy;
    private Long lastTriggerTime;
    private Long nextTriggerTime;
    private Date nextScanTime;
    private Integer scanFailedCount;
    private String remark;
    private Integer version;
    private String updatedBy;
    private String createdBy;

}

