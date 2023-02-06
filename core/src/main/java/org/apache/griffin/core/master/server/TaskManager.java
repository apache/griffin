package org.apache.griffin.core.master.server;


import com.google.common.collect.Maps;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.api.common.GRPCCode;
import org.apache.griffin.api.proto.protocol.ExecuteNodeServiceGrpc;
import org.apache.griffin.api.proto.protocol.SayHelloRequest;
import org.apache.griffin.api.proto.protocol.SayHelloResponse;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
@Slf4j
public class TaskManager {

    public ConcurrentMap<String, ExecuteNodeServiceGrpc.ExecuteNodeServiceBlockingStub> clientMap = Maps.newConcurrentMap();

    public void registerWorker(String hostName, int port) throws UnknownHostException {

        try {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(hostName, port)
                    .usePlaintext()
                    .build();
            ExecuteNodeServiceGrpc.ExecuteNodeServiceBlockingStub clientStub = ExecuteNodeServiceGrpc.newBlockingStub(channel);
            SayHelloResponse sayHelloResponse = clientStub.sayHello(SayHelloRequest.newBuilder().build());
            if (sayHelloResponse.getCode() == GRPCCode.SUCCESS.getCode()) {
                clientMap.put(hostName + "_" + port, clientStub);
            } else  {
                throw new UnknownHostException(hostName + ":" + port);
            }
        } catch (UnknownHostException uhe) {
            throw uhe;
        } catch (Exception e) {
            log.error("Connect to ExecuteNode Failed. Host: {}, Port: {}", hostName, port);
        }
    }

    // todo check client health

    // todo reconnect
}
