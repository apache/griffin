package org.apache.griffin.core.master.server;


import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.api.entity.enums.DQInstanceStatus;
import org.apache.griffin.core.master.context.TaskManagerContext;
import org.apache.griffin.core.master.transport.DQCConnection;
import org.apache.griffin.core.master.transport.DQCConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;

@Component
@Slf4j
public class TaskManager {

    @Autowired
    private DQCConnectionManager dqcConnectionManager;
    @Autowired
    private TaskManagerContext taskManagerContext;

    public void registerWorker(String hostName, int port) throws UnknownHostException {
        try {
            dqcConnectionManager.registerWorker(hostName, port);
        } catch (UnknownHostException uhe) {
            throw uhe;
        } catch (Exception e) {
            log.error("Connect to ExecuteNode Failed. Host: {}, Port: {}", hostName, port);
        }
    }

    public void submitDQTask(Long instanceId) {
        DQCConnection aliveClient = dqcConnectionManager.getAliveClient();
        if (aliveClient == null) {
            log.error("getAliveClient Failed, Please check");
            return;
        }
        if (aliveClient.submitDQTask(instanceId)) {
            taskManagerContext.addTask(instanceId, aliveClient);
            // todo flush to db
        }
    }

    public DQInstanceStatus querySingleDQTask(Long instanceId) throws Exception {
        DQCConnection aliveClient = dqcConnectionManager.getAliveClient();
        if (aliveClient == null) {
            log.error("getAliveClient Failed, Please check");
            return null;
        }
        DQInstanceStatus dqInstanceStatus = aliveClient.querySingleDQTask(instanceId);
        return dqInstanceStatus;
    }

    public boolean stopDQTask(Long instanceId) {
        DQCConnection aliveClient = dqcConnectionManager.getAliveClient();
        if (aliveClient == null) {
            log.error("getAliveClient Failed, Please check");
            return false;
        }
        if (aliveClient.stopDQTask(instanceId)) {
            taskManagerContext.clearTask(instanceId);
            return true;
        }
        return false;
    }
}
