package org.apache.griffin.core.master.server;


import com.google.common.collect.Maps;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.api.common.GRPCCode;
import org.apache.griffin.api.proto.protocol.ExecuteNodeServiceGrpc;
import org.apache.griffin.api.proto.protocol.SayHelloRequest;
import org.apache.griffin.api.proto.protocol.SayHelloResponse;
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

        }
        if (aliveClient.submitDQTask(instanceId)) {
            // todo add task and client info to cache
        }
    }
}
