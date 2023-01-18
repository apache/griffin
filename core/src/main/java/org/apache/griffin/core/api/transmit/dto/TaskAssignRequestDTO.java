package org.apache.griffin.core.api.transmit.dto;

import org.apache.griffin.core.api.transmit.AbstractProtocol;
import org.apache.griffin.core.api.transmit.GriffinProtocolEvent;
import org.apache.griffin.core.api.transmit.ProtocolEventEnums;

public class TaskAssignRequestDTO extends AbstractProtocol {

    @Override
    public ProtocolEventEnums getProtocolEvent() {
        return ProtocolEventEnums.TASK_ASSIGN_REQUEST;
    }
}
