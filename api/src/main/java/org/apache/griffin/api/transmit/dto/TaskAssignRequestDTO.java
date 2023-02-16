package org.apache.griffin.api.transmit.dto;

import org.apache.griffin.api.transmit.AbstractProtocol;
import org.apache.griffin.api.transmit.ProtocolEventEnums;

public class TaskAssignRequestDTO extends AbstractProtocol {

    @Override
    public ProtocolEventEnums getProtocolEvent() {
        return ProtocolEventEnums.TASK_ASSIGN_REQUEST;
    }
}
