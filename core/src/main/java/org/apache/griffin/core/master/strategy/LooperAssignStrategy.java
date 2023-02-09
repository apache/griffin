package org.apache.griffin.core.master.strategy;


import org.apache.griffin.core.master.transport.DQCConnection;

import java.util.List;

public class LooperAssignStrategy extends AbstractAssignStrategy {

    @Override
    public DQCConnection assignTask(List<DQCConnection> clientList) {
        return null;
    }
}
