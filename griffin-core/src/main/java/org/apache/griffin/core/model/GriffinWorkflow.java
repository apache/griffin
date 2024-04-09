package org.apache.griffin.core.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.griffin.common.model.BaseEntity;
import org.apache.griffin.core.enums.RunState;

import java.beans.Transient;

@Getter
@Setter
@NoArgsConstructor
public class GriffinWorkflow extends BaseEntity {
    private Long wfinstanceId;
    private String curNode;
    private String preNode;
    private Integer sequence;
    private Integer runState;
    private Long instanceId;

    public GriffinWorkflow(Long wfinstanceId, String curNode, String preNode, int sequence) {
        this.wfinstanceId = wfinstanceId;
        this.curNode = curNode;
        this.preNode = preNode;
        this.sequence = sequence;
        this.runState = RunState.WAITING.value();
    }

    @Transient
    public boolean isTerminal() {
        return RunState.of(runState).isTerminal();
    }

    @Transient
    public boolean isFailure() {
        return RunState.of(runState).isFailure();
    }

}

