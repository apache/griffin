package org.apache.griffin.metric.exception;

import lombok.Data;
import org.apache.griffin.metric.entity.BaseEntity;

@Data
public class GriffinErrorEntity extends BaseEntity {

    private Integer code;

    private String message;

    public GriffinErrorEntity(GriffinErr err) {
        this.code = err.getCode();
        this.message = err.getMessage();
    }

    public GriffinErrorEntity(GriffinErr err, String details) {
        this.code = err.getCode();
        this.message = String.format(err.getMessage(), details);
    }
}
