package org.apache.griffin.core.master.strategy;


import org.apache.griffin.core.master.transport.DQCConnection;

import java.util.List;

public abstract class AbstractAssignStrategy {

    public abstract DQCConnection assignTask(List<DQCConnection> clientList);
}
